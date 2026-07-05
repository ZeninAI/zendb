//! Ready-made operator implementations (currently placeholders).

mod full_text_index;
mod merkle_tree;
mod rhai;

pub use full_text_index::{FullTextIndexConfig, FullTextIndexOperator};
pub use merkle_tree::{
    MerkleEntry, MerkleLeaf, MerkleNode, MerkleRoot, MerkleTreeConfig, MerkleTreeOperator,
    MerkleTreeStateKey, MerkleTreeStateValue,
};
pub use rhai::{RhaiOperator, RhaiOperatorConfig};

#[doc(hidden)]
#[macro_export]
macro_rules! __zendb_with_prelude_operators {
    ($callback:path, $vis:vis mod $module:ident { $($operators:tt)* }) => {
        $callback! {
            $vis mod $module {
                FullTextIndex($crate::operator::prelude::FullTextIndexOperator),
                MerkleTree($crate::operator::prelude::MerkleTreeOperator),
                Rhai($crate::operator::prelude::RhaiOperator),
                $($operators)*
            }
        }
    };
}
