//! Ready-made operator implementations.

mod full_text_index;
mod merkle_tree;
mod rhai;

pub use full_text_index::{FullTextIndexConfig, FullTextIndexOperator, FullTextPosting};
pub use merkle_tree::{MerkleLeaf, MerkleTreeConfig, MerkleTreeOperator};
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
