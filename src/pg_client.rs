use postgres_protocol::escape::{escape_identifier, escape_literal};
use tokio_postgres::Client;

/// PgClient implements an interface to send LISTEN, UNLISTEN and NOTIFY commands to a PostgreSQL
/// server. LISTEN and NOTIFY don't work with prepared statements so we create the queries ourselves
/// and send without parameters.
pub(crate) struct PgClient {
    client: Client,
}

impl PgClient {
    /// Creates a new PgClient with the default buffer size.
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Listens to a channel.
    pub async fn listen(&self, channel: &str) -> Result<u64, crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        let listen_cmd = format!("LISTEN {channel}");
        self.client.execute(&listen_cmd, &[]).await
    }

    /// Unlistens from a channel.
    pub async fn unlisten(&self, channel: &str) -> Result<u64, crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        let unlisten_cmd = format!("UNLISTEN {channel}");
        self.client.execute(&unlisten_cmd, &[]).await
    }

    /// Sends a notification to a channel.
    pub async fn notify(
        &self,
        channel: &str,
        payload: Option<&str>,
    ) -> Result<u64, crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        let notify_cmd = match payload {
            Some(payload) => {
                let payload = escape_literal(payload);
                format!("NOTIFY {channel}, {payload}")
            }
            None => format!("NOTIFY {channel}"),
        };
        self.client.execute(&notify_cmd, &[]).await
    }

    /// Returns the PostgreSQL process id of this connection.
    pub async fn get_pid(&self) -> Result<i32, crate::tokio_postgres::Error> {
        let row = self
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await?;
        let pid = row.get(0);
        log::debug!("get_pid: {pid}");
        Ok(pid)
    }
}
