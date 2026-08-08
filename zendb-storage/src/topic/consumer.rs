//! Named durable topic consumers and their cursor lifecycle.

use std::io;

use crate::backend::_traits::WriteBackend;
use bincode::Decode;

use super::{
    reader::TopicReader,
    types::{SeekTarget, TopicOffset},
};

/// Named consumer handle over a topic.
pub struct TopicConsumer<T> {
    pub(super) reader: TopicReader<T>,
    pub(super) name: String,
    pub(super) committed: TopicOffset,
}

impl<T> TopicConsumer<T> {
    /// Persist the current reader cursor for this consumer.
    pub fn commit(&mut self) -> io::Result<()> {
        let committed = self.reader.offset;
        self.reader
            .topic
            .offsets
            .lock()
            .put(self.name.clone(), committed)?;
        self.committed = committed;
        if let Some(registration) = self.reader.topic.consumers.lock().get_mut(&self.name) {
            registration.committed = committed;
        }
        Ok(())
    }

    /// Rewind this consumer to its last committed cursor.
    pub fn reset(&mut self) {
        self.reader.set_offset(self.committed);
    }

    /// Delete this consumer's persisted cursor and unregister its name.
    pub fn delete(mut self) -> io::Result<()> {
        self.reader.current = None;
        let mut consumers = self.reader.topic.consumers.lock();
        self.reader.topic.offsets.lock().delete(&self.name)?;
        consumers.remove(&self.name);
        Ok(())
    }

    /// Position this consumer through its reader.
    pub fn seek(&mut self, target: SeekTarget<'_>) -> io::Result<bool> {
        self.reader.seek(target)
    }
}

impl<T> Iterator for TopicConsumer<T>
where
    T: Decode<()>,
{
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader
            .next()
            .map(|result| result.map(|(_, value)| value))
    }
}

impl<T> Drop for TopicConsumer<T> {
    fn drop(&mut self) {
        let mut consumers = self.reader.topic.consumers.lock();
        if let Some(registration) = consumers.get_mut(&self.name) {
            registration.active = false;
        }
    }
}
