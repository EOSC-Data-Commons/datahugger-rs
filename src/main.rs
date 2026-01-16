use datahugger::{OSF, download_with_validation};
use reqwest::ClientBuilder;
use std::sync::Arc;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();
    let client = ClientBuilder::new().build().unwrap();
    let repo = Arc::new(OSF::new(client.clone()));
    download_with_validation(
        // 3ua2c has many files and a large file (>600M)
        // Url::from_str("https://api.osf.io/v2/nodes/3ua2c/files").unwrap(),
        repo,
        "./dummy_tests",
    )
    .await
    .unwrap();
}
