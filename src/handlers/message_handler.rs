use anyhow::Result;
use tracing::info;

pub async fn handle_message(body: &str) -> Result<()> {
    info!("🔨 Processando mensagem: {}", body);

    // Simula processamento
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Aqui vai sua lógica real:
    // - Parse JSON
    // - Chamar API
    // - Salvar no DB
    // - Enviar email
    // - etc.

    info!("✅ Mensagem processada");
    Ok(())
}
