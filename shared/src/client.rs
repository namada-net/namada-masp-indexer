use std::time::Duration;

use tendermint_rpc::HttpClient;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_USER_AGENT_PREFIX: &str = "Namada Masp Indexer";

#[derive(Clone, Debug)]
pub struct Client {
    inner: HttpClient,
}

impl Client {
    pub fn new(ur: &str) -> Self {
        let url = ur.parse().expect("Invalid URL");
        let inner = reqwest::Client::builder()
            .cookie_store(true)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .user_agent(user_agent_string())
            .build()
            .expect("Failed to create HTTP client");
        let http_client = HttpClient::new_from_parts(
            inner,
            url,
            tendermint_rpc::client::CompatMode::V0_37,
        );
        Client { inner: http_client }
    }

    pub fn get(&self) -> HttpClient {
        self.inner.clone()
    }
}

impl AsRef<HttpClient> for Client {
    fn as_ref(&self) -> &HttpClient {
        &self.inner
    }
}

fn user_agent_string() -> String {
    format!("{DEFAULT_USER_AGENT_PREFIX}{}", env!("CARGO_PKG_VERSION"))
}
