mod handlers;

use anyhow::Result;
use futures::StreamExt;
use lapin::{
    options::*,
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind,
};
use std::env;
use std::time::Duration;
use tracing::{info, error, warn, Level};
use tracing_subscriber::FmtSubscriber;

use handlers::message_handler::handle_message;

// ✅ Config simples - sem Deserialize, só dados
#[derive(Debug, Clone)]
struct RabbitMQConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
    vhost: String,
    queue: String,
    exchange: String,
    routing_key: String,
}

#[derive(Debug, Clone)]
struct WorkerConfig {
    prefetch_count: u16,
    reconnect_delay_secs: u64,
}

#[derive(Debug, Clone)]
struct Config {
    rabbitmq: RabbitMQConfig,
    worker: WorkerConfig,
}

// ✅ Carrega config direto das env vars com defaults
fn load_config() -> Config {
    Config {
        rabbitmq: RabbitMQConfig {
            host: env::var("RABBITMQ_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("RABBITMQ_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5672),
            username: env::var("RABBITMQ_USER").unwrap_or_else(|_| "guest".to_string()),
            password: env::var("RABBITMQ_PASS").unwrap_or_else(|_| "guest".to_string()),
            vhost: env::var("RABBITMQ_VHOST").unwrap_or_else(|_| "/".to_string()),
            queue: env::var("RABBITMQ_QUEUE").unwrap_or_else(|_| "chickie_tasks".to_string()),
            exchange: env::var("RABBITMQ_EXCHANGE").unwrap_or_else(|_| "chickie_exchange".to_string()),
            routing_key: env::var("RABBITMQ_ROUTING_KEY").unwrap_or_else(|_| "task.#".to_string()),
        },
        worker: WorkerConfig {
            prefetch_count: env::var("WORKER_PREFETCH")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(10),
            reconnect_delay_secs: env::var("WORKER_RECONNECT_DELAY")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5),
        },
    }
}

fn build_amqp_url(cfg: &RabbitMQConfig) -> String {
    format!(
        "amqp://{}:{}@{}:{}/{}",
        cfg.username, cfg.password, cfg.host, cfg.port, cfg.vhost
    )
}

async fn setup_queue(conn: &Connection, cfg: &RabbitMQConfig) -> Result<()> {
    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            &cfg.exchange,
            ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!("✅ Exchange '{}' declarada", cfg.exchange);

    channel
        .queue_declare(
            &cfg.queue,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!("✅ Queue '{}' declarada", cfg.queue);

    channel
        .queue_bind(
            &cfg.queue,
            &cfg.exchange,
            &cfg.routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!("✅ Queue bindada ao exchange");

    Ok(())
}

async fn consume_messages(
    conn: &Connection,
    rmq_cfg: &RabbitMQConfig,
    worker_cfg: &WorkerConfig,
) -> Result<()> {
    let channel = conn.create_channel().await?;

    channel
        .basic_qos(worker_cfg.prefetch_count, BasicQosOptions::default())
        .await?;

    let mut consumer = channel
        .basic_consume(
            &rmq_cfg.queue,
            "chickie_worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!("✅ Consumer iniciado na queue '{}'", rmq_cfg.queue);

    while let Some(Ok(delivery)) = consumer.next().await {
        let delivery_tag = delivery.delivery_tag;
        let body = String::from_utf8_lossy(&delivery.data);
        info!("📩 Mensagem (tag {}): {}", delivery_tag, body);

        match handle_message(&body).await {
            Ok(_) => {
                channel
                    .basic_ack(delivery_tag, BasicAckOptions { multiple: false })
                    .await?;
            }
            Err(e) => {
                error!("❌ Erro ao processar: {}", e);
                channel
                    .basic_nack(
                        delivery_tag,
                        BasicNackOptions { multiple: false, requeue: true },
                    )
                    .await?;
            }
        }
    }
    Ok(())
}

async fn run_worker(cfg: Config) -> Result<()> {
    let amqp_url = build_amqp_url(&cfg.rabbitmq);
    let safe_url = amqp_url.replace(&cfg.rabbitmq.password, "***");
    info!("🔗 Conectando ao RabbitMQ: {}", safe_url);

    loop {
        match Connection::connect(&amqp_url, ConnectionProperties::default()).await {
            Ok(conn) => {
                info!("✅ Conectado");

                if let Err(e) = setup_queue(&conn, &cfg.rabbitmq).await {
                    error!("❌ Erro no setup: {}", e);
                    tokio::time::sleep(Duration::from_secs(cfg.worker.reconnect_delay_secs)).await;
                    continue;
                }

                if let Err(e) = consume_messages(&conn, &cfg.rabbitmq, &cfg.worker).await {
                    error!("❌ Erro no consumer: {}", e);
                }
            }
            Err(e) => {
                error!("❌ Falha ao conectar: {}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(cfg.worker.reconnect_delay_secs)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_env_filter("chickie_worker=info")
        .init();

    info!("🔧 Chickie Worker iniciando...");

    let cfg = load_config();
    
    info!("📡 Queue: {}", cfg.rabbitmq.queue);
    info!("📡 Exchange: {}", cfg.rabbitmq.exchange);

    if let Err(e) = run_worker(cfg).await {
        error!("❌ Worker falhou: {}", e);
    }

    Ok(())
}