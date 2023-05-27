use crate::tokio_postgres::{Config, MakeTlsConnect, NoTls, Socket};
use either::Either;

#[derive(Clone)]
pub(crate) enum ConnectionParameters {
    ConnectionStr(Box<str>),
    TokioPostgresConfig(Box<Config>),
}

pub struct PgPubSubOptionsBuilder {
    pub(crate) connection_params: ConnectionParameters,
    pub(crate) channel_capacity: usize,
    pub(crate) suppress_own_notifications: bool,
}

#[derive(Clone)]
pub struct PgPubSubOptions<T: MakeTlsConnect<Socket> + Clone> {
    pub(crate) connection_params: ConnectionParameters,
    pub(crate) channel_capacity: usize,
    pub(crate) suppress_own_notifications: bool,
    pub(crate) tls: T,
}

impl PgPubSubOptionsBuilder {
    /// Configuration for connecting to PostgresQL with the given parameters.
    pub fn new(host: &str, dbname: &str, user: &str, password: &str) -> Self {
        let connection_str = Self::build_connection_string(host, dbname, user, password);
        Self::from_connection_str(&connection_str)
    }

    /// Configuration for connecting to PostgreSQL with the given connection string as-is, using one
    /// of two possible formats:
    /// 1) "host=localhost dbname=name_of_database user=name_of_user password=user_password"
    /// 2) "postgresql:///mydb?user=user&host=/var/lib/postgresql"
    ///
    /// See: https://docs.rs/tokio-postgres/0.7.8/tokio_postgres/config/struct.Config.html
    pub fn from_connection_str(connection_str: &str) -> Self {
        let cfg = ConnectionParameters::ConnectionStr(connection_str.into());
        Self::from_connection_params(cfg)
    }

    /// Connects to PostgreSQL with the given tokio_postgres::Config, re-exported by this lib as
    /// `pgpubsub::tokio_postgres::Config`.
    ///
    /// See: https://docs.rs/tokio-postgres/0.7.8/tokio_postgres/config/struct.Config.html
    pub fn from_tokio_postgres_config(config: Config) -> Self {
        let cfg = ConnectionParameters::TokioPostgresConfig(Box::new(config));
        Self::from_connection_params(cfg)
    }

    fn from_connection_params(connection_params: ConnectionParameters) -> Self {
        Self {
            connection_params,
            channel_capacity: 32,
            suppress_own_notifications: false,
        }
    }

    /// Sets the capacities of the channels that deliver notifications and database commands.
    pub fn channel_capacity(self, channel_capacity: usize) -> Self {
        Self {
            channel_capacity,
            ..self
        }
    }

    /// Sets whether notifications that we send ourselves should be received by us. Defaults to no.
    pub fn suppress_own_notifications(self, suppress_own_notifications: bool) -> Self {
        Self {
            suppress_own_notifications,
            ..self
        }
    }

    fn build_connection_string(host: &str, dbname: &str, user: &str, password: &str) -> String {
        // This format consists of space-separated key-value pairs. Values which are either the
        // empty string or contain whitespace should be wrapped in '' and \ characters should be
        // backslash-escaped.
        // https://docs.rs/tokio-postgres/0.7.8/tokio_postgres/config/struct.Config.html
        format!(
            "host={host} dbname={dbname} user={user} password={password}",
            host = LibpqValue::from_str(host),
            dbname = LibpqValue::from_str(dbname),
            user = LibpqValue::from_str(user),
            password = LibpqValue::from_str(password),
        )
    }

    pub fn build(self) -> PgPubSubOptions<NoTls> {
        self.build_with_tls(NoTls)
    }

    /// Build with the given Tls option given as a `tokio_postgres::tls::MakeTlsConnect<Socket>`.
    /// All useful options here should be re-exported by this crate, for example
    /// `pgpubsub::tokio_postgres::NoTls`.
    pub fn build_with_tls<T: MakeTlsConnect<Socket> + Clone>(self, tls: T) -> PgPubSubOptions<T> {
        PgPubSubOptions {
            connection_params: self.connection_params,
            channel_capacity: self.channel_capacity,
            tls,
            suppress_own_notifications: self.suppress_own_notifications,
        }
    }
}

struct LibpqValue<'a> {
    value: Either<String, &'a str>,
}

impl<'a> LibpqValue<'a> {
    #[inline(never)]
    fn from_str(input: &'a str) -> Self {
        let mut contains_spaces = false;
        let mut escape_count = 0;
        for ch in input.chars() {
            contains_spaces |= ch == ' ';
            escape_count += (ch == '\\' || ch == '\'') as usize;
        }
        if escape_count == 0 && !contains_spaces {
            return LibpqValue {
                value: Either::Right(input),
            };
        }

        let output_len = input
            .len()
            .checked_add(escape_count)
            .and_then(|len| len.checked_add(2 * contains_spaces as usize))
            .expect("Escaped String will exceed the maximum length");

        let mut output = String::with_capacity(output_len);

        let quote = contains_spaces || input.is_empty();

        if quote {
            output.push('\''); // The leading single quote.
        }

        if escape_count == 0 {
            // Nothing to escape, just copy all of input into output.
            output.push_str(input);
        } else {
            // Otherwise go through each character in input and escape if needed.
            for ch in input.chars() {
                if ch == '\\' || ch == '\'' {
                    output.push('\\');
                    output.push(ch);
                } else {
                    output.push(ch);
                }
            }
        }

        if quote {
            output.push('\''); // The trailing single quote.
        }

        debug_assert_eq!(output.len(), output_len);

        LibpqValue {
            value: Either::Left(output),
        }
    }

    pub fn as_str(&'a self) -> &'a str {
        match &self.value {
            Either::Left(s) => s,
            Either::Right(s) => s,
        }
    }
}

impl From<LibpqValue<'_>> for String {
    fn from(v: LibpqValue<'_>) -> Self {
        match v.value {
            Either::Left(s) => s,
            Either::Right(s) => s.to_owned(),
        }
    }
}

impl std::fmt::Display for LibpqValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{s}", s = self.as_str())
    }
}

#[cfg(test)]
mod test {
    use std::ptr;

    use super::*;

    #[test]
    fn lib_pq_value_fmt_unescaped_and_unquoted_shares_memory() {
        let input = "secret123";
        let v = LibpqValue::from_str(input);
        let output = v.as_str();
        // input and output should share the same data if there was no escaping or quoting.
        assert!(ptr::eq(input, output))
    }

    #[test]
    fn lib_pq_value_fmt_escaped() {
        let input = r#"secret\123"#;
        let v = LibpqValue::from_str(input);
        let output = v.as_str();
        // input and output do not share the same data since the string had to be escaped.
        assert!(!ptr::eq(input, output));
        // The backslash should have been escaped so that there are now double backslashes.
        assert_eq!(output, r#"secret\\123"#);
    }

    #[test]
    fn lib_pq_value_fmt_quoted() {
        let input = "secret 123";
        let v = LibpqValue::from_str(input);
        let output = v.as_str();
        // input and output do not share the same data since the string had to be escaped.
        assert!(!ptr::eq(input, output));
        // The string should be enclosed in single quotes.
        assert_eq!(output, "'secret 123'");
    }

    #[test]
    fn format_libpq_string() {
        let host = r#"\\PGHOST\"#;
        let dbname = "databasename";
        let user = "user";
        let password = r#"1j( \'9f"#;
        let con_str = PgPubSubOptionsBuilder::build_connection_string(host, dbname, user, password);

        let expected = r#"host=\\\\PGHOST\\ dbname=databasename user=user password='1j( \\\'9f'"#;

        assert_eq!(con_str, expected);
    }
}
