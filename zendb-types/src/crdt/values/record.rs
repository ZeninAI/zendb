//! Movable-tree record container with self-healing child lookup.

use crate::{EventTime, Type, TypeMetadata, TypeTag, Value, zendb_container_type};
use bincode::{Decode, Encode};
use std::collections::BTreeMap;

zendb_container_type! {
    #[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
    pub struct Record { fields: BTreeMap<std::string::String, Value> }

    impl Record {
        pub fn op_clear(&mut self, remote: crate::EventTime) -> bool {
            if remote <= self.__event_time { return false; }
            self.fields.clear();
            self.__event_time = remote;
            true
        }

        pub fn op_insert(&mut self, remote: EventTime, name: std::string::String, mut value: Value) -> bool {
            self.__event_time = self.__event_time.max(remote);
            if self
                .fields
                .get(&name)
                .is_some_and(|current| remote <= current.event_time())
            {
                return false;
            }
            value.set_event_time(remote);
            value.set_tombstone(false);
            self.fields.insert(name, value);
            true
        }

        pub fn ensure_child(&mut self, remote: EventTime, field: &std::string::String, expected: TypeTag) -> Option<&mut Value> {
            self.__event_time = self.__event_time.max(remote);
            let child = self.fields.entry(field.clone()).or_insert_with(|| expected.empty_value());
            if child.type_tag() != expected {
                if remote <= child.event_time() { return None; }
                *child = expected.empty_value();
            }
            Some(child)
        }

    }
}

impl Record {
    pub fn from_fields(fields: impl IntoIterator<Item = (std::string::String, Value)>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
            ..Self::default()
        }
    }
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.fields.get(field)
    }
    pub fn len(&self) -> usize {
        self.fields.len()
    }
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
    pub fn fields(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.fields.iter().map(|(key, value)| (key.as_str(), value))
    }
}
