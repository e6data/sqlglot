//! Relocate request-owned discovery tokens without copying their Python objects.
#![deny(missing_docs, clippy::pedantic)]

use crate::token::Token;
use pyo3::prelude::*;
use rustc_hash::FxHashSet;

struct Relocation {
    token: Py<Token>,
    start: usize,
    end: usize,
    line: usize,
    col: usize,
}

/// Validate all chunks before updating token positions in place.
///
/// Chunks must contain disjoint token objects owned by the current request.
/// Incompatible lexing returns `None`, leaving all tokens untouched.
#[pyfunction(name = "_remap_tokens")]
// PyO3 extracts Python sets into owned Rust collections at this boundary.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn remap_tokens(
    py: Python<'_>,
    chunks: Vec<(Vec<Py<Token>>, isize)>,
    body: &str,
    source: &str,
    different_words: FxHashSet<String>,
    quoted_types: FxHashSet<u16>,
) -> PyResult<Option<Vec<Py<Token>>>> {
    let mut newlines = Vec::new();
    let mut chars = body.chars().peekable();
    let mut length = 0;
    while let Some(character) = chars.next() {
        if character == '\n' || (character == '\r' && chars.peek() != Some(&'\n')) {
            newlines.push(length);
        }
        length += 1;
    }
    let mut quote_positions: Option<FxHashSet<usize>> = None;
    let capacity = chunks.iter().map(|(tokens, _)| tokens.len()).sum();
    let mut pending = Vec::with_capacity(capacity);
    for (tokens, shift) in chunks {
        for token in tokens {
            let t = token.try_borrow(py)?;
            if !quoted_types.contains(&t.token_type) {
                let text = t.text.bind(py).to_str()?;
                // Non-ASCII case conversion is deliberately left to normal lexing.
                if !text.is_ascii() {
                    return Ok(None);
                }
                let word = text.to_ascii_uppercase();
                if different_words.contains(&word) {
                    return Ok(None);
                }
                if matches!(word.as_str(), "B" | "E" | "X") {
                    let positions = quote_positions.get_or_insert_with(|| {
                        source
                            .chars()
                            .enumerate()
                            .filter_map(|(i, c)| (c == '\'').then_some(i))
                            .collect()
                    });
                    if positions.contains(&(t.end + 1)) {
                        return Ok(None);
                    }
                }
            }
            let Some(start) = t.start.checked_add_signed(shift) else {
                return Ok(None);
            };
            let Some(end) = t.end.checked_add_signed(shift) else {
                return Ok(None);
            };
            if start > end || end >= length {
                return Ok(None);
            }
            let line = newlines.partition_point(|position| *position < end) + 1;
            let col = if line > 1 {
                end - newlines[line - 2]
            } else {
                end + 1
            };
            drop(t);
            pending.push(Relocation {
                token,
                start,
                end,
                line,
                col,
            });
        }
    }
    let mut result = Vec::with_capacity(pending.len());
    for relocation in pending {
        let mut token = relocation.token.try_borrow_mut(py)?;
        token.start = relocation.start;
        token.end = relocation.end;
        token.line = relocation.line;
        token.col = relocation.col;
        drop(token);
        result.push(relocation.token);
    }
    Ok(Some(result))
}
