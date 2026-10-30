use pyo3::prelude::*;
use pyo3::{pymodule, types::PyModule, Bound, PyResult};
use settings::{TokenTypeSettings, TokenizerDialectSettings, TokenizerSettings};
use token::Token;
use tokenizer::Tokenizer;

pub mod settings;
pub mod token;
pub mod tokenizer;
pub mod trie;
mod regions;

#[pymodule]
fn sqlglotrs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(regions::remap_tokens, m)?)?;
    m.add_class::<Token>()?;
    m.add_class::<TokenTypeSettings>()?;
    m.add_class::<TokenizerSettings>()?;
    m.add_class::<TokenizerDialectSettings>()?;
    m.add_class::<Tokenizer>()?;
    Ok(())
}
