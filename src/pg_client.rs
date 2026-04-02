use postgres_protocol::escape::{escape_identifier, escape_literal};
use tokio_postgres::Client;

/// PgClient implements an interface to send LISTEN, UNLISTEN and NOTIFY commands to a PostgreSQL
/// server.
pub(crate) struct PgClient {
    client: Client,
}

impl PgClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Listens to a channel.
    pub async fn listen(&self, channel: &str) -> Result<(), crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        self.client.execute(&format!("LISTEN {channel}"), &[]).await?;
        Ok(())
    }

    /// Unlistens from a channel.
    pub async fn unlisten(&self, channel: &str) -> Result<(), crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        self.client.execute(&format!("UNLISTEN {channel}"), &[]).await?;
        Ok(())
    }

    /// Sends a notification to a channel.
    pub async fn notify(
        &self,
        channel: &str,
        payload: Option<&str>,
    ) -> Result<(), crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        let cmd = match payload {
            Some(payload) => {
                let payload = escape_literal(payload);
                format!("NOTIFY {channel}, {payload}")
            }
            None => format!("NOTIFY {channel}"),
        };
        self.client.execute(&cmd, &[]).await?;
        Ok(())
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
