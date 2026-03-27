use anyhow::Result;
use futures::StreamExt;
use lapin::{
    options::*,
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind,
};
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

// ═══════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════

/// O handler recebe o body da mensagem e retorna Result
type HandlerFn = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Configuração de uma fila registrada
#[derive(Clone)]
struct QueueBinding {
    queue: String,
    routing_key: String,
    handler: HandlerFn,
}

/// Configurações gerais do worker
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub vhost: String,
    pub exchange: String,
    pub prefetch_count: u16,
    pub reconnect_delay: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5672,
            username: "guest".into(),
            password: "guest".into(),
            vhost: "/".into(),
            exchange: "chickie_exchange".into(),
            prefetch_count: 10,
            reconnect_delay: Duration::from_secs(5),
        }
    }
}

impl WorkerConfig {
    /// Carrega config das env vars, com fallback pro default
    pub fn from_env() -> Self {
        let def = Self::default();
        Self {
            host: env::var("RABBITMQ_HOST")
                .expect("RABBITMQ_HOST não encontrado!"),
            port: env::var("RABBITMQ_PORT").ok().and_then(|p| p.parse().ok())
                .expect("RABBITMQ_PORT não encontrado!"),
            username: env::var("RABBITMQ_USER")
                .expect("RABBITMQ_USER não encontrado!"),
            password: env::var("RABBITMQ_PASS")
                .expect("RABBITMQ_PASS não encontrado!"),
            vhost: env::var("RABBITMQ_VHOST")
                .expect("RABBITMQ_VHOST não encontrado!"),
            exchange: env::var("RABBITMQ_EXCHANGE")
                .expect("RABBITMQ_EXCHANGE não encontrado!"),
            prefetch_count: env::var("WORKER_PREFETCH").ok().and_then(|p| p.parse().ok())
                .unwrap_or(def.prefetch_count),
            reconnect_delay: env::var("WORKER_RECONNECT_DELAY")
                .ok()
                .and_then(|p| p.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(def.reconnect_delay),
        }
    }

    fn amqp_url(&self) -> String {
        format!(
            "amqp://{}:{}@{}:{}/{}",  // porta omitida por conta de dns configurado no traefik
            self.username, self.password, self.host, self.port, self.vhost
        )
    }

    fn safe_url(&self) -> String {
        self.amqp_url().replace(&self.password, "***")
    }
}

// ═══════════════════════════════════════════════════════════════
// Worker (o "app" estilo Flask)
// ═══════════════════════════════════════════════════════════════

pub struct Worker {
    config: WorkerConfig,
    bindings: Vec<QueueBinding>,
}

impl Worker {
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            config,
            bindings: Vec::new(),
        }
    }

    /// Registra um handler para uma fila + routing key.
    ///
    /// ```rust
    /// worker.queue("emails", "task.email.#", |body| async move {
    ///     println!("processando email: {body}");
    ///     Ok(())
    /// }); 
    /// ```
    pub fn queue<F, Fut>(&mut self, queue: &str, routing_key: &str, handler: F) -> &mut Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.bindings.push(QueueBinding {
            queue: queue.to_string(),
            routing_key: routing_key.to_string(),
            handler: Arc::new(move |body| Box::pin(handler(body))),
        });
        self
    }

    /// Inicia o worker — loop infinito com reconnect automático.
    pub async fn run(self) -> Result<()> {
        let url = self.config.amqp_url();
        info!("🔗 RabbitMQ: {}", self.config.safe_url());
        info!("📡 Exchange: {}", self.config.exchange);

        for b in &self.bindings {
            info!("📋 Registrado: queue='{}' routing_key='{}'", b.queue, b.routing_key);
        }

        loop {
            match Connection::connect(&url, ConnectionProperties::default()).await {
                Ok(conn) => {
                    info!("✅ Conectado ao RabbitMQ");

                    if let Err(e) = self.setup_and_consume(&conn).await {
                        error!("❌ Erro: {e}");
                    }
                }
                Err(e) => {
                    error!("❌ Falha ao conectar: {e}");
                }
            }

            info!("🔄 Reconectando em {:?}...", self.config.reconnect_delay);
            tokio::time::sleep(self.config.reconnect_delay).await;
        }
    }

    async fn setup_and_consume(&self, conn: &Connection) -> Result<()> {
        let channel = conn.create_channel().await?;

        // Declara exchange
        channel
            .exchange_declare(
                &self.config.exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        // Declara e binda cada fila
        for b in &self.bindings {
            channel
                .queue_declare(&b.queue, QueueDeclareOptions::default(), FieldTable::default())
                .await?;

            channel
                .queue_bind(
                    &b.queue,
                    &self.config.exchange,
                    &b.routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;

            info!("✅ Queue '{}' pronta (routing: '{}')", b.queue, b.routing_key);
        }

        // Monta lookup: queue -> handler
        let handlers: HashMap<String, HandlerFn> = self
            .bindings
            .iter()
            .map(|b| (b.queue.clone(), b.handler.clone()))
            .collect();

        // Um consumer por fila, todos na mesma channel
        channel
            .basic_qos(self.config.prefetch_count, BasicQosOptions::default())
            .await?;

        let mut consumers = Vec::new();
        for b in &self.bindings {
            let consumer_tag = format!("chickie_{}", b.queue);
            let consumer = channel
                .basic_consume(
                    &b.queue,
                    &consumer_tag,
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await?;
            consumers.push((b.queue.clone(), consumer));
        }

        // Spawna uma task por consumer
        let mut handles = Vec::new();
        for (queue_name, mut consumer) in consumers {
            let ch = conn.create_channel().await?;
            let handler = handlers.get(&queue_name).cloned().unwrap();
            let qname = queue_name.clone();

            let handle = tokio::spawn(async move {
                while let Some(Ok(delivery)) = consumer.next().await {
                    let tag = delivery.delivery_tag;
                    let body = String::from_utf8_lossy(&delivery.data).to_string();

                    info!("[{}] 📩 tag={}: {}", qname, tag, body);

                    match (handler)(body).await {
                        Ok(_) => {
                            if let Err(e) = ch.basic_ack(tag, BasicAckOptions::default()).await {
                                error!("[{}] ❌ Erro no ack: {e}", qname);
                            }
                        }
                        Err(e) => {
                            error!("[{}] ❌ Erro no handler: {e}", qname);
                            let _ = ch
                                .basic_nack(tag, BasicNackOptions { multiple: false, requeue: true })
                                .await;
                        }
                    }
                }
            });

            handles.push(handle);
        }

        // Espera qualquer consumer cair (= desconexão)
        let (result, _index, _remaining) = futures::future::select_all(handles).await;
        match result {
            Ok(()) => info!("⚠️ Consumer encerrou normalmente"),
            Err(e) => error!("❌ Consumer panicked: {e}"),
        }
        Ok(())
    }
}