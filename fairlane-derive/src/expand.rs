use proc_macro2::{Literal, Span, TokenStream};
use quote::quote;
use syn::{
  Attribute, Data, DeriveInput, Fields, Ident, LitStr, Path, Variant, ext::IdentExt,
  spanned::Spanned,
};

/// The `impl fairlane::Lane` for `input`, or every refusal found, combined.
pub(crate) fn lane(input: &DeriveInput) -> syn::Result<TokenStream> {
  let Data::Enum(data) = &input.data else {
    return Err(syn::Error::new_spanned(
      &input.ident,
      "`Lane` can only be derived for an enum",
    ));
  };

  let mut errors = Errors::default();
  let krate = container(&input.attrs, &mut errors);

  if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
    errors.push(syn::Error::new_spanned(
      &input.generics,
      "`Lane` cannot be derived for an enum with generic parameters",
    ));
  }
  if data.variants.is_empty() {
    errors.push(syn::Error::new_spanned(
      &input.ident,
      "`Lane` needs at least one variant: a vocabulary with no lane makes no channel",
    ));
  }

  let mut lanes: Vec<LaneVariant<'_>> = Vec::with_capacity(data.variants.len());
  for variant in &data.variants {
    if let Some(lane) = lane_variant(variant, &mut errors) {
      if let Some(express) = lane.express {
        if lanes.iter().any(|earlier| earlier.express.is_none()) {
          errors.push(syn::Error::new(
            express,
            "an express lane must come before every lane that is not express",
          ));
        }
      }
      if let Some(earlier) = lanes.iter().find(|earlier| earlier.name == lane.name) {
        errors.push(syn::Error::new(
          lane.name_span,
          format!(
            "the lane name \"{}\" is given to `{}` already",
            lane.name, earlier.ident
          ),
        ));
      }
      lanes.push(lane);
    }
  }
  errors.finish()?;

  let ident = &input.ident;
  let krate = krate.unwrap_or_else(|| syn::parse_quote!(::fairlane));
  let count = Literal::usize_unsuffixed(lanes.len());
  let indices: Vec<Literal> = (0..lanes.len()).map(Literal::usize_unsuffixed).collect();
  let variants: Vec<&Ident> = lanes.iter().map(|lane| lane.ident).collect();
  let names: Vec<&str> = lanes.iter().map(|lane| lane.name.as_str()).collect();
  let express: Vec<bool> = lanes.iter().map(|lane| lane.express.is_some()).collect();
  let messages: Vec<String> = lanes
    .iter()
    .enumerate()
    .map(|(index, lane)| format!("`{ident}::{}` must have discriminant {index}", lane.ident))
    .collect();
  // A const panic cannot format the limit, so the message names the count and the cause; the
  // limit itself has one source, the crate's `__private::MAX_LANES`.
  let too_many = format!(
    "`{ident}` has {} lanes, more than a fairlane channel holds (see `Lane::N`)",
    lanes.len()
  );

  Ok(quote! {
    #[automatically_derived]
    impl #krate::Lane for #ident {
      type N = #krate::__private::typenum::U<#count>;

      #[inline]
      fn index(self) -> ::core::primitive::usize {
        self as ::core::primitive::usize
      }

      #[inline]
      fn from_index(index: ::core::primitive::usize) -> ::core::option::Option<Self> {
        match index {
          #( #indices => ::core::option::Option::Some(Self::#variants), )*
          _ => ::core::option::Option::None,
        }
      }

      #[inline]
      fn as_str(self) -> &'static ::core::primitive::str {
        match self {
          #( Self::#variants => #names, )*
        }
      }

      #[inline]
      fn is_express(self) -> ::core::primitive::bool {
        match self {
          #( Self::#variants => #express, )*
        }
      }
    }

    #[allow(clippy::assertions_on_constants)]
    const _: () = {
      #( ::core::assert!(#ident::#variants as ::core::primitive::usize == #indices, #messages); )*
      ::core::assert!(#count <= #krate::__private::MAX_LANES, #too_many);
    };
  })
}

/// One variant, read.
struct LaneVariant<'a> {
  ident: &'a Ident,
  name: String,
  name_span: Span,
  /// The span of the `express` word, where it is given.
  express: Option<Span>,
}

fn lane_variant<'a>(variant: &'a Variant, errors: &mut Errors) -> Option<LaneVariant<'a>> {
  let mut fine = true;
  if !matches!(variant.fields, Fields::Unit) {
    errors.push(syn::Error::new_spanned(
      &variant.fields,
      "a lane is a unit variant: it cannot carry fields",
    ));
    fine = false;
  }
  if let Some((_, discriminant)) = &variant.discriminant {
    errors.push(syn::Error::new_spanned(
      discriminant,
      "a lane takes its index from its place in the declaration: remove the discriminant",
    ));
    fine = false;
  }

  let mut express: Option<Span> = None;
  let mut name: Option<LitStr> = None;
  for attr in fairlane_attrs(&variant.attrs) {
    let parsed = attr.parse_nested_meta(|meta| {
      if meta.path.is_ident("express") {
        if express.is_some() {
          return Err(meta.error("`express` is given twice"));
        }
        express = Some(meta.path.span());
        Ok(())
      } else if meta.path.is_ident("name") {
        if name.is_some() {
          return Err(meta.error("`name` is given twice"));
        }
        let value: LitStr = meta.value()?.parse()?;
        if value.value().is_empty() {
          return Err(syn::Error::new(
            value.span(),
            "a lane's name cannot be empty",
          ));
        }
        name = Some(value);
        Ok(())
      } else {
        Err(meta.error(format!(
          "unknown word `{}` on a variant: a variant takes `express` and `name = \"…\"`",
          path_text(&meta.path)
        )))
      }
    });
    if let Err(error) = parsed {
      errors.push(error);
      fine = false;
    }
  }

  fine.then(|| {
    let (name, name_span) = match name {
      Some(name) => (name.value(), name.span()),
      None => (
        snake_case(&variant.ident.unraw().to_string()),
        variant.ident.span(),
      ),
    };
    LaneVariant {
      ident: &variant.ident,
      name,
      name_span,
      express,
    }
  })
}

/// The enum's own words: `crate = "…"`.
fn container(attrs: &[Attribute], errors: &mut Errors) -> Option<Path> {
  let mut krate: Option<Path> = None;
  for attr in fairlane_attrs(attrs) {
    let parsed = attr.parse_nested_meta(|meta| {
      if meta.path.is_ident("crate") {
        if krate.is_some() {
          return Err(meta.error("`crate` is given twice"));
        }
        let value: LitStr = meta.value()?.parse()?;
        krate = Some(value.parse::<Path>().map_err(|_| {
          syn::Error::new(
            value.span(),
            "`crate` takes a path to fairlane, such as \"my_crate::fairlane\"",
          )
        })?);
        Ok(())
      } else {
        Err(meta.error(format!(
          "unknown word `{}` on the enum: the enum takes `crate = \"…\"`",
          path_text(&meta.path)
        )))
      }
    });
    if let Err(error) = parsed {
      errors.push(error);
    }
  }
  krate
}

fn fairlane_attrs(attrs: &[Attribute]) -> impl Iterator<Item = &Attribute> {
  attrs.iter().filter(|attr| attr.path().is_ident("fairlane"))
}

fn path_text(path: &Path) -> String {
  path
    .segments
    .iter()
    .map(|segment| segment.ident.unraw().to_string())
    .collect::<Vec<_>>()
    .join("::")
}

/// `HttpServer` and `HTTPServer` read `http_server`; `V2Lane` reads `v2_lane`.
pub(crate) fn snake_case(name: &str) -> String {
  let chars: Vec<char> = name.chars().collect();
  let mut out = String::with_capacity(name.len() + 4);
  for (at, &c) in chars.iter().enumerate() {
    if c.is_uppercase() {
      let boundary = at > 0 && {
        let before = chars[at - 1];
        let after_is_lower = chars.get(at + 1).is_some_and(|next| next.is_lowercase());
        before != '_'
          && (before.is_lowercase()
            || before.is_ascii_digit()
            || (before.is_uppercase() && after_is_lower))
      };
      if boundary {
        out.push('_');
      }
      out.extend(c.to_lowercase());
    } else {
      out.push(c);
    }
  }
  out
}

/// Every refusal of one derive, reported together.
#[derive(Default)]
struct Errors(Option<syn::Error>);

impl Errors {
  fn push(&mut self, error: syn::Error) {
    match &mut self.0 {
      Some(first) => first.combine(error),
      None => self.0 = Some(error),
    }
  }

  fn finish(self) -> syn::Result<()> {
    self.0.map_or(Ok(()), Err)
  }
}

#[cfg(test)]
mod tests;
