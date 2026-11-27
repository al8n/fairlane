//! The derive's refusals, as the compiler shows them. trybuild runs in CI only: `cargo test -p
//! fairlane --features derive --test derive_ui -- --ignored`. The same refusals are unit tests of
//! the expansion in fairlane-derive, which run everywhere.

#[test]
#[ignore = "trybuild runs in CI only"]
fn refusals() {
  trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
