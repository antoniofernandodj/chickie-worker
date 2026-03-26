use anyhow::Result;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

use chickie_worker::{Worker, WorkerConfig};


async fn handle_email(body: String) -> Result<()> {
    info!("📧 Processando email: {body}");
    // sua lógica aqui...
    Ok(())
}

async fn handle_notification(body: String) -> Result<()> {
    info!("🔔 Processando notificação: {body}");
    Ok(())
}

async fn handle_report(body: String) -> Result<()> {
    info!("📊 Gerando relatório: {body}");
    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🐣 Chickie Worker iniciando...");

    let mut worker =
        Worker::new(WorkerConfig::from_env());

    // ✨ Registra handlers — simples como Flask routes
    worker
        .queue(
            "emails",
            "task.email.#",
            handle_email
        )
        .queue(
            "notifications",
            "task.notification.#",
            handle_notification
        )
        .queue(
            "reports",
            "task.report.#",
            handle_report
        );

    worker.queue(
        "logs",
        "task.log.#",
        |body| async move {
            info!("📝 Log recebido: {body}");
            Ok(())
    });

    worker.run().await
}
