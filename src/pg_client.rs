use std::fmt::Write;

use postgres_protocol::escape::{escape_identifier, escape_literal};
use tokio_postgres::Client;

/// PgClient implements an interface to send LISTEN, UNLISTEN and NOTIFY commands to a PostgreSQL
/// server. Uses `batch_execute` (simple query protocol) to avoid unnecessary prepared statement
/// overhead for these non-parameterizable commands.
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
        self.client.batch_execute(&format!("LISTEN {channel}")).await
    }

    /// Unlistens from a channel.
    pub async fn unlisten(&self, channel: &str) -> Result<(), crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        self.client.batch_execute(&format!("UNLISTEN {channel}")).await
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
        self.client.batch_execute(&cmd).await
    }

    /// Sends a batch of notifications in a single round-trip, wrapped in a transaction so
    /// that either all of them are delivered or none are. Empty input is a no-op.
    pub async fn notify_batch(
        &self,
        items: &[(&str, Option<&str>)],
    ) -> Result<(), crate::tokio_postgres::Error> {
        let Some(cmd) = build_notify_batch_sql(items) else {
            return Ok(());
        };
        self.client.batch_execute(&cmd).await
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

/// Builds the SQL for a `notify_batch` call, or returns `None` for an empty input. Pulled
/// out so we can unit-test the SQL we'd send without touching a real Postgres.
fn build_notify_batch_sql(items: &[(&str, Option<&str>)]) -> Option<String> {
    if items.is_empty() {
        return None;
    }
    let mut cmd = String::from("BEGIN;");
    for (channel, payload) in items {
        let channel = escape_identifier(channel);
        match payload {
            Some(payload) => {
                let payload = escape_literal(payload);
                let _ = write!(&mut cmd, "NOTIFY {channel}, {payload};");
            }
            None => {
                let _ = write!(&mut cmd, "NOTIFY {channel};");
            }
        }
    }
    cmd.push_str("COMMIT;");
    Some(cmd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch_returns_none() {
        assert!(build_notify_batch_sql(&[]).is_none());
    }

    #[test]
    fn single_item_with_payload_is_wrapped_in_transaction() {
        let sql = build_notify_batch_sql(&[("foo", Some("hello"))]).unwrap();
        assert!(sql.starts_with("BEGIN;"));
        assert!(sql.ends_with("COMMIT;"));
        assert!(sql.contains("NOTIFY \"foo\", 'hello';"));
    }

    #[test]
    fn single_item_without_payload() {
        let sql = build_notify_batch_sql(&[("foo", None)]).unwrap();
        assert!(sql.contains("NOTIFY \"foo\";"));
        assert!(!sql.contains(", "));
    }

    #[test]
    fn multiple_items_concatenated_in_order() {
        let sql = build_notify_batch_sql(&[
            ("a", Some("1")),
            ("b", None),
            ("c", Some("3")),
        ])
        .unwrap();
        let expected = "BEGIN;NOTIFY \"a\", '1';NOTIFY \"b\";NOTIFY \"c\", '3';COMMIT;";
        assert_eq!(sql, expected);
    }

    #[test]
    fn channel_and_payload_are_escaped() {
        // Single quote in the payload must be doubled by escape_literal.
        let sql = build_notify_batch_sql(&[("ch", Some("a'b"))]).unwrap();
        assert!(sql.contains("'a''b'"), "got: {sql}");
        // Double quote in the channel name must be doubled by escape_identifier.
        let sql = build_notify_batch_sql(&[("ch\"x", None)]).unwrap();
        assert!(sql.contains("\"ch\"\"x\""), "got: {sql}");
    }
}
