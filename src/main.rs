use datahugger::dispatch::QueryRepository;
use datahugger::download_with_validation;
use datahugger::repo_impl::{DataverseDataset, DataverseFile, OSF};
use reqwest::ClientBuilder;
use std::str::FromStr;
use std::sync::Arc;
use tracing_subscriber::FmtSubscriber;
use url::Url;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();
    let client = ClientBuilder::new().build().unwrap();

    // in osf.io, '3ua2c' has many files and a large file (>600M)
    let query_repo = QueryRepository {
        repo: Arc::new(OSF::new()),
        record_id: "3ua2c".to_string(),
    };
    // TODO: download action as method from blanket trait
    download_with_validation(&client, query_repo, "./dummy_tests")
        .await
        .unwrap();

    // doi:10.7910/DVN/KBHLOD
    let base_url = Url::from_str("https://dataverse.harvard.edu/").unwrap();
    let version = ":latest-published".to_string();
    let query_repo = QueryRepository {
        repo: Arc::new(DataverseDataset::new(base_url, version)),
        record_id: "doi:10.7910/DVN/KBHLOD".to_string(),
    };
    download_with_validation(&client, query_repo, "./dummy_tests")
        .await
        .unwrap();

    // doi:10.7910/DVN/KBHLOD/DHJ45U
    let base_url = Url::from_str("https://dataverse.harvard.edu/").unwrap();
    let version = ":latest-published".to_string();
    let query_repo = QueryRepository {
        repo: Arc::new(DataverseFile::new(base_url, version)),
        record_id: "doi:10.7910/DVN/KBHLOD/DHJ45U".to_string(),
    };
    download_with_validation(&client, query_repo, "./dummy_tests")
        .await
        .unwrap();
}
