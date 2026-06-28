use bincode::{Decode, Encode};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OperatorPhase {
    Active,
    Finished,
    Failed { error: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorStatus {
    Continue,
    Finish,
}
