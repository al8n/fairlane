use syn::parse_quote;

use super::*;

fn refusal(input: DeriveInput) -> String {
  match lane(&input) {
    Ok(tokens) => panic!("expected a refusal, got: {tokens}"),
    Err(error) => error
      .into_iter()
      .map(|error| error.to_string())
      .collect::<Vec<_>>()
      .join("\n"),
  }
}

fn expansion(input: DeriveInput) -> String {
  lane(&input).unwrap().to_string()
}

#[test]
fn expands_a_plain_enum() {
  let out = expansion(parse_quote! {
    enum Urgency { #[fairlane(express)] Wake, Interactive, #[fairlane(name = "bulk")] Batch }
  });
  assert!(out.contains(":: fairlane :: Lane for Urgency"), "{out}");
  assert!(
    out.contains("type N = :: fairlane :: __private :: typenum :: U < 3 >"),
    "{out}"
  );
  assert!(out.contains("Self :: Wake => \"wake\""), "{out}");
  assert!(out.contains("Self :: Batch => \"bulk\""), "{out}");
  assert!(out.contains("Self :: Wake => true"), "{out}");
  assert!(out.contains("Self :: Interactive => false"), "{out}");
  assert!(
    out.contains("2 => :: core :: option :: Option :: Some (Self :: Batch)"),
    "{out}"
  );
  assert!(
    out.contains("\"`Urgency::Batch` must have discriminant 2\""),
    "{out}"
  );
}

#[test]
fn roots_paths_at_the_crate_word() {
  let out = expansion(parse_quote! {
    #[fairlane(crate = "resident::fairlane")]
    enum Band { One }
  });
  assert!(
    out.contains("resident :: fairlane :: Lane for Band"),
    "{out}"
  );
  assert!(
    out.contains("resident :: fairlane :: __private :: typenum :: U < 1 >"),
    "{out}"
  );
}

#[test]
fn refuses_a_struct() {
  let message = refusal(parse_quote! { struct Urgency; });
  assert_eq!(message, "`Lane` can only be derived for an enum");
}

#[test]
fn refuses_generics() {
  let message = refusal(parse_quote! { enum Urgency<T> { One(T) } });
  assert!(
    message.contains("`Lane` cannot be derived for an enum with generic parameters"),
    "{message}"
  );
}

#[test]
fn refuses_an_empty_enum() {
  let message = refusal(parse_quote! { enum Urgency {} });
  assert!(
    message.contains("`Lane` needs at least one variant"),
    "{message}"
  );
}

#[test]
fn refuses_fields() {
  let message = refusal(parse_quote! { enum Urgency { One, Two(u8) } });
  assert_eq!(message, "a lane is a unit variant: it cannot carry fields");
}

#[test]
fn refuses_a_discriminant() {
  let message = refusal(parse_quote! { enum Urgency { One = 1, Two } });
  assert_eq!(
    message,
    "a lane takes its index from its place in the declaration: remove the discriminant"
  );
}

#[test]
fn refuses_express_behind_plain() {
  let message = refusal(parse_quote! {
    enum Urgency { #[fairlane(express)] Wake, Interactive, #[fairlane(express)] Late }
  });
  assert_eq!(
    message,
    "an express lane must come before every lane that is not express"
  );
}

#[test]
fn refuses_a_name_given_twice() {
  let message = refusal(parse_quote! {
    enum Urgency { #[fairlane(name = "x")] One, #[fairlane(name = "x")] Two }
  });
  assert_eq!(message, "the lane name \"x\" is given to `One` already");
  let message = refusal(parse_quote! {
    enum Urgency { BatchJob, #[fairlane(name = "batch_job")] Other }
  });
  assert_eq!(
    message,
    "the lane name \"batch_job\" is given to `BatchJob` already"
  );
}

#[test]
fn refuses_an_empty_name() {
  let message = refusal(parse_quote! { enum Urgency { #[fairlane(name = "")] One } });
  assert_eq!(message, "a lane's name cannot be empty");
}

#[test]
fn refuses_unknown_words() {
  let message = refusal(parse_quote! { enum Urgency { #[fairlane(urgent)] One } });
  assert_eq!(
    message,
    "unknown word `urgent` on a variant: a variant takes `express` and `name = \"…\"`"
  );
  let message = refusal(parse_quote! { #[fairlane(express)] enum Urgency { One } });
  assert_eq!(
    message,
    "unknown word `express` on the enum: the enum takes `crate = \"…\"`"
  );
  let message = refusal(parse_quote! { enum Urgency { #[fairlane(crate = "x")] One } });
  assert!(
    message.starts_with("unknown word `crate` on a variant"),
    "{message}"
  );
}

#[test]
fn refuses_a_word_given_twice() {
  let message = refusal(parse_quote! { enum Urgency { #[fairlane(express, express)] One } });
  assert_eq!(message, "`express` is given twice");
  let message = refusal(parse_quote! {
    enum Urgency { #[fairlane(name = "a")] #[fairlane(name = "b")] One }
  });
  assert_eq!(message, "`name` is given twice");
  let message = refusal(parse_quote! {
    #[fairlane(crate = "a")] #[fairlane(crate = "b")] enum Urgency { One }
  });
  assert_eq!(message, "`crate` is given twice");
}

#[test]
fn refuses_a_crate_that_is_not_a_path() {
  let message = refusal(parse_quote! { #[fairlane(crate = "not a path")] enum Urgency { One } });
  assert_eq!(
    message,
    "`crate` takes a path to fairlane, such as \"my_crate::fairlane\""
  );
}

#[test]
fn reports_every_refusal_at_once() {
  let message = refusal(parse_quote! { enum Urgency { One(u8), Two = 2 } });
  assert_eq!(message.lines().count(), 2, "{message}");
}

#[test]
fn snake_case_names() {
  assert_eq!(snake_case("Interactive"), "interactive");
  assert_eq!(snake_case("BatchJob"), "batch_job");
  assert_eq!(snake_case("HTTPServer"), "http_server");
  assert_eq!(snake_case("V2Lane"), "v2_lane");
  assert_eq!(snake_case("Already_Snake"), "already_snake");
  assert_eq!(snake_case("A"), "a");
}
