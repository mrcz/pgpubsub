use std::fmt::Write;

use postgres_protocol::escape::{escape_identifier, escape_literal};
use tokio::sync::RwLock;
use tokio_postgres::Client;

/// PgClient implements an interface to send LISTEN, UNLISTEN and NOTIFY commands to a PostgreSQL
/// server. Uses `batch_execute` (simple query protocol) to avoid unnecessary prepared statement
/// overhead for these non-parameterizable commands.
///
/// The inner client sits behind an `RwLock` so the listener task can swap in a fresh
/// client after a reconnect ([`PgClient::replace`]) while commands keep taking cheap
/// read locks. Operations issued while the connection is down fail fast with a
/// connection-closed error rather than waiting for the reconnect.
pub(crate) struct PgClient {
    client: RwLock<Client>,
}

impl PgClient {
    pub fn new(client: Client) -> Self {
        Self {
            client: RwLock::new(client),
        }
    }

    /// Swaps in the client of a freshly established connection (after a reconnect).
    pub async fn replace(&self, client: Client) {
        *self.client.write().await = client;
    }

    /// Listens to a channel.
    pub async fn listen(&self, channel: &str) -> Result<(), crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        let client = self.client.read().await;
        client.batch_execute(&format!("LISTEN {channel}")).await
    }

    /// Unlistens from a channel.
    pub async fn unlisten(&self, channel: &str) -> Result<(), crate::tokio_postgres::Error> {
        let channel = escape_identifier(channel);
        let client = self.client.read().await;
        client.batch_execute(&format!("UNLISTEN {channel}")).await
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
        let client = self.client.read().await;
        client.batch_execute(&cmd).await
    }

    /// Sends a batch of notifications in a single round-trip. The statements share the
    /// implicit transaction of the multi-statement simple query, so either all of them
    /// are delivered or none are. Empty input is a no-op.
    pub async fn notify_batch(
        &self,
        items: &[(&str, Option<&str>)],
    ) -> Result<(), crate::tokio_postgres::Error> {
        let Some(cmd) = build_notify_batch_sql(items) else {
            return Ok(());
        };
        let client = self.client.read().await;
        client.batch_execute(&cmd).await
    }
}

/// Returns the PostgreSQL process id of the given client's connection. Takes the raw
/// client because it is used during connection setup, before the client is published
/// via [`PgClient`].
pub(crate) async fn fetch_backend_pid(
    client: &Client,
) -> Result<i32, crate::tokio_postgres::Error> {
    let row = client.query_one("SELECT pg_backend_pid()", &[]).await?;
    let pid = row.get(0);
    log::debug!("fetch_backend_pid: {pid}");
    Ok(pid)
}

/// Builds one `LISTEN` statement per channel for re-establishing subscriptions after a
/// reconnect, or `None` when there is nothing to re-listen to.
pub(crate) fn build_relisten_sql<'a>(
    channels: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let mut cmd = String::new();
    for channel in channels {
        let channel = escape_identifier(channel);
        let _ = write!(&mut cmd, "LISTEN {channel};");
    }
    (!cmd.is_empty()).then_some(cmd)
}

/// Builds the SQL for a `notify_batch` call, or returns `None` for an empty input. Pulled
/// out so we can unit-test the SQL we'd send without touching a real Postgres.
///
/// Deliberately no explicit `BEGIN`/`COMMIT`: the statements of a multi-statement simple
/// query already run in one implicit transaction, which gives the all-or-nothing
/// delivery we want. An explicit `BEGIN` would *persist* past the message, so an error
/// mid-batch would leave the session in an aborted transaction block and every later
/// command on this shared connection would fail with "current transaction is aborted".
fn build_notify_batch_sql(items: &[(&str, Option<&str>)]) -> Option<String> {
    if items.is_empty() {
        return None;
    }
    let mut cmd = String::new();
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
    fn single_item_with_payload() {
        let sql = build_notify_batch_sql(&[("foo", Some("hello"))]).unwrap();
        assert_eq!(sql, "NOTIFY \"foo\", 'hello';");
    }

    #[test]
    fn batch_has_no_explicit_transaction_commands() {
        // An explicit BEGIN would outlive the simple-query message and leave the session
        // in an aborted transaction block if any statement failed. The implicit
        // transaction of the multi-statement message provides the atomicity instead.
        let sql = build_notify_batch_sql(&[("a", Some("1")), ("b", None)]).unwrap();
        assert!(!sql.contains("BEGIN"), "got: {sql}");
        assert!(!sql.contains("COMMIT"), "got: {sql}");
    }

    #[test]
    fn single_item_without_payload() {
        let sql = build_notify_batch_sql(&[("foo", None)]).unwrap();
        assert!(sql.contains("NOTIFY \"foo\";"));
        assert!(!sql.contains(", "));
    }

    #[test]
    fn multiple_items_concatenated_in_order() {
        let sql =
            build_notify_batch_sql(&[("a", Some("1")), ("b", None), ("c", Some("3"))]).unwrap();
        let expected = "NOTIFY \"a\", '1';NOTIFY \"b\";NOTIFY \"c\", '3';";
        assert_eq!(sql, expected);
    }

    #[test]
    fn relisten_sql_is_empty_for_no_channels() {
        assert!(build_relisten_sql([]).is_none());
    }

    #[test]
    fn relisten_sql_lists_each_channel_escaped() {
        let sql = build_relisten_sql(["foo", "we\"ird"]).unwrap();
        assert_eq!(sql, "LISTEN \"foo\";LISTEN \"we\"\"ird\";");
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
