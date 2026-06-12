//! Integration tests against a real PostgreSQL instance.
//!
//! These are `#[ignore]`d by default because they need the database from
//! `example/docker-compose.yml`:
//!
//! ```sh
//! docker compose -f example/docker-compose.yml up -d
//! cargo test -- --ignored
//! ```

use std::time::Duration;

use pgpubsub::tokio_postgres::NoTls;
use pgpubsub::{PgPubSub, PgPubSubOptionsBuilder, PubSubError};
use tokio::time::timeout;

const CONN_STR: &str = "host=localhost dbname=auth user=auth password=auth";

const RECV_TIMEOUT: Duration = Duration::from_secs(5);

async fn connect(suppress_own: bool) -> PgPubSub {
    let options = PgPubSubOptionsBuilder::from_connection_str(CONN_STR)
        .suppress_own_notifications(suppress_own)
        .build();
    PgPubSub::connect(options)
        .await
        .expect("failed to connect; is the docker-compose database running?")
}

#[tokio::test]
#[ignore = "requires PostgreSQL from example/docker-compose.yml"]
async fn notify_roundtrip() {
    let pubsub = connect(false).await;
    let mut sub = pubsub.listen("it_roundtrip").await.unwrap();

    pubsub.notify("it_roundtrip", Some("hello")).await.unwrap();

    let n = timeout(RECV_TIMEOUT, sub.recv()).await.unwrap().unwrap();
    assert_eq!(&*n.channel, "it_roundtrip");
    assert_eq!(&*n.payload, "hello");
}

#[tokio::test]
#[ignore = "requires PostgreSQL from example/docker-compose.yml"]
async fn notify_batch_delivers_all() {
    let pubsub = connect(false).await;
    let mut sub_a = pubsub.listen("it_batch_a").await.unwrap();
    let mut sub_b = pubsub.listen("it_batch_b").await.unwrap();

    pubsub
        .notify_batch(&[("it_batch_a", Some("1")), ("it_batch_b", None)])
        .await
        .unwrap();

    let a = timeout(RECV_TIMEOUT, sub_a.recv()).await.unwrap().unwrap();
    assert_eq!(&*a.payload, "1");
    let b = timeout(RECV_TIMEOUT, sub_b.recv()).await.unwrap().unwrap();
    assert_eq!(&*b.payload, "");
}

#[tokio::test]
#[ignore = "requires PostgreSQL from example/docker-compose.yml"]
async fn own_notifications_are_suppressed() {
    let suppressed = connect(true).await;
    let other = connect(false).await;
    let mut sub = suppressed.listen("it_suppress").await.unwrap();

    // A notification from our own connection must not be delivered...
    suppressed.notify("it_suppress", Some("own")).await.unwrap();
    assert!(
        timeout(Duration::from_secs(1), sub.recv()).await.is_err(),
        "own notification was not suppressed"
    );

    // ...but one from another connection must be.
    other.notify("it_suppress", Some("other")).await.unwrap();
    let n = timeout(RECV_TIMEOUT, sub.recv()).await.unwrap().unwrap();
    assert_eq!(&*n.payload, "other");
}

#[tokio::test]
#[ignore = "requires PostgreSQL from example/docker-compose.yml"]
async fn oversized_payload_is_rejected_and_connection_survives() {
    let pubsub = connect(false).await;
    let mut sub = pubsub.listen("it_oversize").await.unwrap();

    let too_big = "x".repeat(8000);
    let err = pubsub
        .notify("it_oversize", Some(&too_big))
        .await
        .expect_err("8000-byte payload should be rejected");
    assert!(matches!(err, PubSubError::InvalidPayload));

    // The connection must still be fully usable afterwards.
    pubsub.notify("it_oversize", Some("after")).await.unwrap();
    let n = timeout(RECV_TIMEOUT, sub.recv()).await.unwrap().unwrap();
    assert_eq!(&*n.payload, "after");
}

/// Kills the pub/sub backend server-side and verifies the client reconnects,
/// re-establishes its LISTENs, and resumes delivery — this is the regression test for
/// the silent-hang-on-dead-connection bug.
#[tokio::test]
#[ignore = "requires PostgreSQL from example/docker-compose.yml"]
async fn reconnects_and_relistens_after_backend_termination() {
    let pubsub = connect(false).await;
    let mut sub = pubsub.listen("it_reconnect").await.unwrap();

    // Learn our backend PID from a self-notification (suppression is off).
    pubsub
        .notify("it_reconnect", Some("pid probe"))
        .await
        .unwrap();
    let probe = timeout(RECV_TIMEOUT, sub.recv()).await.unwrap().unwrap();
    let backend_pid = probe.process_id;

    // Terminate that backend from an independent connection.
    let (admin, admin_conn) = tokio_postgres::connect(CONN_STR, NoTls).await.unwrap();
    tokio::spawn(admin_conn);
    admin
        .execute("SELECT pg_terminate_backend($1)", &[&backend_pid])
        .await
        .unwrap();

    // The client reconnects with backoff and re-LISTENs. Poll by publishing from the
    // admin connection until a notification arrives on the existing subscription.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        admin
            .batch_execute("NOTIFY it_reconnect, 'after reconnect'")
            .await
            .unwrap();
        match timeout(Duration::from_millis(500), sub.recv()).await {
            Ok(Ok(n)) if &*n.payload == "after reconnect" => break,
            _ if tokio::time::Instant::now() > deadline => {
                panic!("subscription did not resume after backend termination")
            }
            _ => {}
        }
    }
}
