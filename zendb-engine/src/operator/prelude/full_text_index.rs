use std::{collections::BTreeSet, io};

use bincode::{Decode, Encode};
use zendb_storage::core::traits::Backend;
use zendb_types::{Cell, PrimaryKey};

use crate::{
    BoxFuture, Change, DispatchOperator, Operator, OperatorContext, OperatorDirective, StateConfig,
    StateHandle,
};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct FullTextIndexConfig {
    pub state: String,
    pub min_token_len: u32,
}

impl Default for FullTextIndexConfig {
    fn default() -> Self {
        Self {
            state: "operator/prelude/full-text-index".to_owned(),
            min_token_len: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct FullTextPosting {
    pub table: String,
    pub key: PrimaryKey,
}

pub struct FullTextIndexOperator {
    state: Option<StateHandle<String, Vec<FullTextPosting>>>,
}

impl Operator for FullTextIndexOperator {
    type Config = FullTextIndexConfig;
    type Timer = ();

    fn new(_config: &Self::Config) -> io::Result<Self> {
        Ok(Self { state: None })
    }

    fn open<'a, D>(
        &'a mut self,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            self.state = Some(ctx.state(&ctx.config().state, Some(StateConfig::default()))?);
            Ok(OperatorDirective::Continue)
        })
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            let state = self
                .state
                .as_ref()
                .expect("full-text index state must be initialized by open")
                .get()?;
            let mut state = state.write();

            for change in changes {
                let posting = FullTextPosting {
                    table: change.event.table_id.clone(),
                    key: change.event.primary_key.clone(),
                };

                for token in tokens(change.previous.as_ref(), ctx.config().min_token_len) {
                    remove_posting(&mut state, &token, &posting)?;
                }
                for token in tokens(change.current.as_ref(), ctx.config().min_token_len) {
                    add_posting(&mut state, &token, posting.clone())?;
                }
            }

            Ok(OperatorDirective::Continue)
        })
    }
}

fn add_posting(
    state: &mut zendb_storage::frontend::state::State<String, Vec<FullTextPosting>>,
    token: &str,
    posting: FullTextPosting,
) -> io::Result<()> {
    state.update(&token.to_owned(), |current| {
        let mut postings: BTreeSet<_> = current.unwrap_or_default().into_iter().collect();
        postings.insert(posting);
        Some(postings.into_iter().collect())
    })
}

fn remove_posting(
    state: &mut zendb_storage::frontend::state::State<String, Vec<FullTextPosting>>,
    token: &str,
    posting: &FullTextPosting,
) -> io::Result<()> {
    state.update(&token.to_owned(), |current| {
        let mut postings: BTreeSet<_> = current.unwrap_or_default().into_iter().collect();
        postings.remove(posting);
        if postings.is_empty() {
            None
        } else {
            Some(postings.into_iter().collect())
        }
    })
}

fn tokens(cell: Option<&Cell>, min_len: u32) -> BTreeSet<String> {
    let Some(cell) = cell else {
        return BTreeSet::new();
    };
    let text = format!("{cell:?}");
    tokenize(&text, min_len)
}

fn tokenize(text: &str, min_len: u32) -> BTreeSet<String> {
    let min_len = min_len as usize;
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter_map(|raw| {
            let token = raw.to_lowercase();
            (token.len() >= min_len).then_some(token)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizer_normalizes_and_filters_short_tokens() {
        let tokens = tokenize("Hello, HELLO db-v2 x", 2);

        assert!(tokens.contains("hello"));
        assert!(tokens.contains("db"));
        assert!(tokens.contains("v2"));
        assert!(!tokens.contains("x"));
        assert_eq!(tokens.iter().filter(|token| *token == "hello").count(), 1);
    }
}
