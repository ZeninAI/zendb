//! Persistent single-writer, multiple-reader append-only topic.
//!
//! The topic is divided by responsibility: configuration and shared state,
//! segment files, the owning writer, and the reader/consumer cursors.

mod consumer;
mod reader;
mod segment;
mod types;
mod writer;

pub use consumer::TopicConsumer;
pub use reader::TopicReader;
pub use types::{SeekTarget, TopicConfig, TopicOffset, TopicStats};
pub use writer::Topic;
