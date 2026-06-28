use bincode::{Decode, Encode};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum Subscription {
    All,
    Exact(String),
    Prefix(String),
    Suffix(String),
    Segments {
        prefix: Option<String>,
        middle: Vec<String>,
        suffix: Option<String>,
    },
}

impl Subscription {
    pub fn pattern(pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 1 {
            return Self::Exact(pattern);
        }

        let prefix = parts
            .first()
            .and_then(|part| (!part.is_empty()).then(|| (*part).to_owned()));
        let suffix = parts
            .last()
            .and_then(|part| (!part.is_empty()).then(|| (*part).to_owned()));
        let middle: Vec<String> = parts[1..parts.len() - 1]
            .iter()
            .filter(|part| !part.is_empty())
            .map(|part| (*part).to_owned())
            .collect();

        match (prefix, middle, suffix) {
            (None, middle, None) if middle.is_empty() => Self::All,
            (Some(prefix), middle, None) if middle.is_empty() => Self::Prefix(prefix),
            (None, middle, Some(suffix)) if middle.is_empty() => Self::Suffix(suffix),
            (prefix, middle, suffix) => Self::Segments {
                prefix,
                middle,
                suffix,
            },
        }
    }

    pub(crate) fn matches(&self, table: &str) -> bool {
        match self {
            Self::All => true,
            Self::Exact(exact) => exact == table,
            Self::Prefix(prefix) => table.starts_with(prefix),
            Self::Suffix(suffix) => table.ends_with(suffix),
            Self::Segments {
                prefix,
                middle,
                suffix,
            } => {
                let mut remaining = table;
                if let Some(prefix) = prefix {
                    let Some(rest) = remaining.strip_prefix(prefix) else {
                        return false;
                    };
                    remaining = rest;
                }

                for segment in middle {
                    match remaining.find(segment) {
                        Some(pos) => remaining = &remaining[pos + segment.len()..],
                        None => return false,
                    }
                }

                suffix
                    .as_ref()
                    .is_none_or(|suffix| remaining.ends_with(suffix))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            initial_delay_ms: 100,
            max_delay_ms: 30_000,
            jitter_factor: 0.25,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct OperatorRuntimeConfig {
    pub subscriptions: Vec<Subscription>,
    pub retry: RetryConfig,
    pub poll_size: usize,
}

impl Default for OperatorRuntimeConfig {
    fn default() -> Self {
        Self {
            subscriptions: Vec::new(),
            retry: RetryConfig::default(),
            poll_size: 128,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_exact() {
        assert!(Subscription::pattern("users").matches("users"));
        assert!(!Subscription::pattern("users").matches("posts"));
    }

    #[test]
    fn glob_star_all() {
        assert!(Subscription::pattern("*").matches("users"));
        assert!(Subscription::pattern("*").matches(""));
    }

    #[test]
    fn glob_prefix() {
        assert!(Subscription::pattern("wiki-*").matches("wiki-pages"));
        assert!(Subscription::pattern("wiki-*").matches("wiki-"));
        assert!(!Subscription::pattern("wiki-*").matches("other"));
    }

    #[test]
    fn glob_suffix() {
        assert!(Subscription::pattern("*-log").matches("users-log"));
        assert!(!Subscription::pattern("*-log").matches("users-data"));
    }

    #[test]
    fn glob_multi_star() {
        assert!(Subscription::pattern("user-*-data").matches("user-john-data"));
        assert!(!Subscription::pattern("user-*-data").matches("user-john-other"));
    }
}
