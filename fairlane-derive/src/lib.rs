//! `#[derive(Lane)]` for [fairlane](https://docs.rs/fairlane). Use it through fairlane's `derive`
//! feature, which re-exports it beside the trait; fairlane's documentation of `Lane` lists its
//! words and refusals.

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod expand;

/// Implements `fairlane::Lane` for a fieldless enum. See fairlane's documentation.
#[proc_macro_derive(Lane, attributes(fairlane))]
pub fn derive_lane(input: TokenStream) -> TokenStream {
  let input = parse_macro_input!(input as DeriveInput);
  expand::lane(&input)
    .unwrap_or_else(syn::Error::into_compile_error)
    .into()
}
