#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use url::Url;

use anyhow::anyhow;
use reqwest::Client;
use std::str::FromStr;

use crate::{Checksum, DirMeta, Entry, Repository, json_get, repo::FileMeta};

// https://osf.io/
// API root url at https://api.osf.io/v2/nodes/
#[derive(Debug)]
pub struct OSF {
    client: Client,
}

impl OSF {
    #[must_use]
    pub fn new(client: Client) -> Self {
        OSF { client }
    }
}

#[async_trait]
impl Repository for OSF {
    fn root_url(&self, id: &str) -> Url {
        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        // - `path_segments_mut` cannot fail for this URL scheme
        let mut url = Url::parse("https://api.osf.io/v2/nodes/").unwrap();
        url.path_segments_mut().unwrap().extend([id, "files"]);
        url
    }

    fn client(&self) -> Client {
        self.client.clone()
    }

    async fn list(&self, dir: DirMeta) -> anyhow::Result<Vec<Entry>> {
        let resp: JsonValue = self
            .client
            .get(dir.api_url.clone())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let files = resp
            .get("data")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| anyhow!("data not resolve to an array"))?;

        let mut entries = Vec::with_capacity(files.len());
        for filej in files {
            let name: String = json_get(filej, "attributes.name")?;
            let kind: String = json_get(filej, "attributes.kind")?;
            match kind.as_ref() {
                "file" => {
                    let size: u64 = json_get(filej, "attributes.size")?;
                    let download_url: String = json_get(filej, "links.download")?;
                    let download_url = Url::from_str(&download_url)?;
                    let hash: String = json_get(filej, "attributes.extra.hashes.sha256")?;
                    let checksum = Checksum::Sha256(hash);
                    let file =
                        FileMeta::new(dir.join(&name), download_url, Some(size), vec![checksum]);
                    entries.push(Entry::File(file));
                }
                "folder" => {
                    let api_url: String =
                        json_get(filej, "relationships.files.links.related.href")?;
                    let api_url = Url::from_str(&api_url)?;
                    let dir = DirMeta::new(api_url, dir.join(&name));
                    entries.push(Entry::Dir(dir));
                }
                _ => Err(anyhow::anyhow!("kind is not 'file' or 'folder'"))?,
            }
        }

        Ok(entries)
    }
}
