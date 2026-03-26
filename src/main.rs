mod handlers;

use anyhow::Result;
use lapin::{
    options::*,
    types::FieldTable,
    Connection, ConnectionProperties, Consumer,
};
use futures::StreamExt; // ← Necessário para o consumer stream
use serde::Deserialize;
use std::env;
use std::time::Duration;
use tracing::{info, error, warn, Level};
use tracing_subscriber::FmtSubscriber;

use handlers::message_handler::handle_message;

#[derive(Debug, Deserialize, Clone)]
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

#[derive(Debug, Deserialize, Clone)]
struct WorkerConfig {
    prefetch_count: u16,
    reconnect_delay_secs: u64,
}

#[derive(Debug, Deserialize)]
struct Config {
    rabbitmq: RabbitMQConfig,
    worker: WorkerConfig,
}

fn get_config_path() -> String {
    if let Ok(path) = env::var("CONFIG_PATH") {
        return path;
    }
    "config.toml".to_string()
}

fn load_config(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(path)?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}

fn build_amqp_url(config: &RabbitMQConfig) -> String {
    format!(
        "amqp://{}:{}@{}:{}/{}",
        config.username,
        config.password,
        config.host,
        config.port,
        config.vhost
    )
}

async fn setup_queue(conn: &Connection, config: &RabbitMQConfig) -> Result<()> {
    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            &config.exchange,
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!("✅ Exchange '{}' declarada", config.exchange);

    channel
        .queue_declare(
            &config.queue,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!("✅ Queue '{}' declarada", config.queue);

    channel
        .queue_bind(
            &config.queue,
            &config.exchange,
            &config.routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!("✅ Queue bindada ao exchange");

    Ok(())
}

async fn consume_messages(
    conn: &Connection,
    config: &RabbitMQConfig,
    worker_config: &WorkerConfig,
) -> Result<()> {
    let channel = conn.create_channel().await?;

    channel
        .basic_qos(
            worker_config.prefetch_count,
            BasicQosOptions::default(),
        )
        .await?;
    info!("✅ Prefetch count: {}", worker_config.prefetch_count);

    // ✅ CORREÇÃO: Consumer agora é um Stream
    let mut consumer = channel
        .basic_consume(
            &config.queue,
            "chickie_worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!("✅ Consumer iniciado na queue '{}'", config.queue);

    // ✅ CORREÇÃO: Usar StreamExt::next() ao invés de recv()
    while let Some(Ok(delivery)) = consumer.next().await {
        let delivery_tag = delivery.delivery_tag;
        let body = String::from_utf8_lossy(&delivery.data);
        
        info!("📩 Mensagem recebida (tag {}): {}", delivery_tag, body);

        match handle_message(&body).await {
            Ok(_) => {
                channel
                    .basic_ack(delivery_tag, BasicAckOptions { multiple: false })
                    .await?;
                info!("✅ Mensagem acked");
            }
            Err(e) => {
                error!("❌ Erro ao processar mensagem: {}", e);
                channel
                    .basic_nack(
                        delivery_tag,
                        BasicNackOptions { multiple: false, requeue: true }
                    )
                    .await?;
                warn!("⚠️  Mensagem nack'd e requeuada");
            }
        }
    }

    Ok(())
}

async fn run_worker(config: Config) -> Result<()> {
    let amqp_url = build_amqp_url(&config.rabbitmq);
    info!("🔗 Conectando ao RabbitMQ: {}", amqp_url.replace(&config.rabbitmq.password, "***"));

    loop {
        match Connection::connect(&amqp_url, ConnectionProperties::default()).await {
            Ok(conn) => {
                info!("✅ Conectado ao RabbitMQ");

                if let Err(e) = setup_queue(&conn, &config.rabbitmq).await {
                    error!("❌ Erro ao setup queue: {}", e);
                    tokio::time::sleep(Duration::from_secs(config.worker.reconnect_delay_secs)).await;
                    continue;
                }

                match consume_messages(&conn, &config.rabbitmq, &config.worker).await {
                    Ok(_) => break,
                    Err(e) => {
                        error!("❌ Erro no consumer: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("❌ Falha ao conectar: {}", e);
            }
        }

        info!("🔄 Reconectando em {}s...", config.worker.reconnect_delay_secs);
        tokio::time::sleep(Duration::from_secs(config.worker.reconnect_delay_secs)).await;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_env_filter("chickie_worker=info")
        .init();

    let config_path = get_config_path();
    info!("🔧 Chickie Worker iniciando...");
    info!("📄 Config: {}", config_path);

    let config = match load_config(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!("❌ Falha ao carregar config: {}", e);
            return Ok(());
        }
    };

    info!("📡 Queue: {}", config.rabbitmq.queue);
    info!("📡 Exchange: {}", config.rabbitmq.exchange);

    if let Err(e) = run_worker(config).await {
        error!("❌ Worker falhou: {}", e);
    }

    Ok(())
}