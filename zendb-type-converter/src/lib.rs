//! Type-directed implementation of `#[derive(CellCodec)]`.
//!
//! The derive turns a Rust type into ZenDB's recursive `Cell` representation
//! without requiring per-field conversion code. It is re-exported by
//! `zendb-types`, so applications normally depend on that crate rather than
//! this implementation crate directly.
//!
//! # Supported shapes
//!
//! - Named structs become `Record` values whose field names match the Rust
//!   field names.
//! - Single-field tuple structs delegate to the inner type, which makes them
//!   useful for strongly typed scalar newtypes.
//! - Unit enums become snake_case `String` values.
//!
//! Field types select their default CRDT codecs automatically. `Option<T>`
//! fields are omitted when `None` and decode a missing or tombstoned record
//! field as `None`.
//!
//! # CRDT ambiguity
//!
//! `BTreeSet<T>` uses the LWW `Set` CRDT by default. Select the
//! additive-wins `OrSet` explicitly when that is the intended protocol
//! semantics:
//!
//! ```ignore
//! #[derive(zendb_types::CellCodec)]
//! struct Device {
//!     roles: BTreeSet<Role>,
//!     #[cell(crdt = "or_set")]
//!     capabilities: BTreeSet<Capability>,
//! }
//! ```
//!
//! A field can select any codec whose `Rust` type matches the field with
//! `#[cell(codec = "path::to::Codec")]`. This is required when one Rust type
//! has multiple valid CRDT representations.
//!
//! Unsupported shapes, including maps and non-byte vectors, are rejected by
//! the derive at compile time. A cell received from another replica can still
//! be malformed, so decoding reports `CellCodecError` at runtime.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, Attribute, Data, DeriveInput, Error, Fields, GenericArgument, Ident, LitStr,
    PathArguments, Result, Type,
};

/// Derive a canonical ZenDB CRDT codec.
///
/// The derive emits `CrdtCodec` and `DefaultCrdtCodec` implementations plus
/// inherent `to_cell` / `from_cell` forwarding methods. See this crate's
/// module documentation for supported Rust shapes and codec-selection hints.
#[proc_macro_derive(CellCodec, attributes(cell))]
pub fn derive_cell_codec(input: TokenStream) -> TokenStream {
    match derive(parse_macro_input!(input as DeriveInput)) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn derive(input: DeriveInput) -> Result<proc_macro2::TokenStream> {
    match input.data {
        Data::Struct(data) => derive_struct(input.ident, input.generics, data.fields),
        Data::Enum(data) => derive_enum(input.ident, input.generics, data.variants.into_iter().collect()),
        Data::Union(_) => Err(Error::new_spanned(
            input.ident,
            "CellCodec cannot be derived for unions; use a named struct, single-field tuple newtype, or unit enum",
        )),
    }
}

fn derive_struct(
    ident: Ident,
    generics: syn::Generics,
    fields: Fields,
) -> Result<proc_macro2::TokenStream> {
    match fields {
        Fields::Named(fields) => derive_record(ident, generics, fields.named.into_iter().collect()),
        Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
            let field = fields.unnamed.into_iter().next().expect("one field");
            if !field.attrs.is_empty() {
                return Err(Error::new_spanned(
                    field.attrs.first().expect("attribute exists"),
                    "CellCodec tuple newtypes do not accept field attributes",
                ));
            }
            derive_newtype(ident, generics, field.ty)
        }
        Fields::Unnamed(fields) => Err(Error::new_spanned(
            fields,
            "CellCodec only supports single-field tuple newtypes",
        )),
        Fields::Unit => Err(Error::new_spanned(
            ident,
            "CellCodec unit structs are unsupported; use a unit enum for a stable string value",
        )),
    }
}

fn derive_record(
    ident: Ident,
    generics: syn::Generics,
    fields: Vec<syn::Field>,
) -> Result<proc_macro2::TokenStream> {
    let mut encoders = Vec::new();
    let mut decoders = Vec::new();
    let mut initializers = Vec::new();

    for field in fields {
        let field_ident = field.ident.expect("named field");
        reject_unsupported_type(&field.ty)?;
        let field_name = LitStr::new(&field_ident.to_string(), field_ident.span());
        let codec_selection = field_codec(&field.attrs)?;
        // Options affect the enclosing Record rather than having their own
        // Cell representation: None is encoded by omitting the field.
        let (optional, value_type) = option_inner(&field.ty);
        if matches!(codec_selection, CodecSelection::OrSet) && !is_btree_set(value_type) {
            return Err(Error::new_spanned(
                value_type,
                "#[cell(crdt = \"or_set\")] requires a BTreeSet<T> field",
            ));
        }

        let codec = codec_type_tokens(codec_selection, value_type)?;
        let encode = quote! {
            <#codec as ::zendb_types::CrdtCodec>::encode(&value.#field_ident, hlc)
        };
        let optional_encode = quote! {
            <#codec as ::zendb_types::CrdtCodec>::encode(value, hlc)
        };
        let decode = quote! {
            {
                let value = cell.value.as_ref()
                    .ok_or_else(|| ::zendb_types::CellCodecError::expected("live Cell"))?;
                <#codec as ::zendb_types::CrdtCodec>::decode(value)
            }
        };

        if optional {
            encoders.push(quote! {
                if let Some(value) = &value.#field_ident {
                    fields.push((
                        #field_name.into(),
                        ::zendb_types::Cell {
                            value: Some(#optional_encode),
                            hlc,
                            sync: ::zendb_types::SyncPolicy::Inherit,
                        },
                    ));
                }
            });
            decoders.push(quote! {
                let #field_ident = match record.get(#field_name).filter(|cell| !cell.is_tombstone()) {
                    Some(cell) => Some(#decode
                        .map_err(|error| error.with_field(#field_name))?),
                    None => None,
                };
            });
        } else {
            encoders.push(quote! {
                fields.push((
                    #field_name.into(),
                    ::zendb_types::Cell {
                        value: Some(#encode),
                        hlc,
                        sync: ::zendb_types::SyncPolicy::Inherit,
                    },
                ));
            });
            decoders.push(quote! {
                let cell = record.get(#field_name)
                    .ok_or(::zendb_types::CellCodecError::MissingField(#field_name))?;
                let #field_ident = #decode.map_err(|error| error.with_field(#field_name))?;
            });
        }
        initializers.push(field_ident);
    }

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::zendb_types::CrdtCodec for #ident #type_generics #where_clause {
            type Rust = Self;

            fn encode(value: &Self, hlc: ::zendb_types::Hlc) -> ::zendb_types::Value {
                let mut fields = Vec::new();
                #(#encoders)*
                ::zendb_types::Value::Record(::zendb_types::crdt::values::Record::from_fields(fields))
            }

            fn decode(value: &::zendb_types::Value) -> Result<Self, ::zendb_types::CellCodecError> {
                let ::zendb_types::Value::Record(record) = value else {
                    return Err(::zendb_types::CellCodecError::expected("Record"));
                };
                #(#decoders)*
                Ok(Self { #(#initializers,)* })
            }
        }

        impl #impl_generics ::zendb_types::DefaultCrdtCodec for #ident #type_generics #where_clause {
            type Codec = Self;
        }

        impl #impl_generics #ident #type_generics #where_clause {
            pub fn to_cell(&self, hlc: ::zendb_types::Hlc) -> ::zendb_types::Cell {
                <Self as ::zendb_types::CellCodec>::to_cell(self, hlc)
            }

            pub fn from_cell(cell: &::zendb_types::Cell) -> Result<Self, ::zendb_types::CellCodecError> {
                <Self as ::zendb_types::CellCodec>::from_cell(cell)
            }
        }
    })
}

fn derive_newtype(
    ident: Ident,
    generics: syn::Generics,
    inner: Type,
) -> Result<proc_macro2::TokenStream> {
    reject_unsupported_type(&inner)?;
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    // Newtypes preserve the underlying representation and primary-key
    // conversion, while still making the domain type distinct to Rust.
    Ok(quote! {
        impl #impl_generics ::zendb_types::CrdtCodec for #ident #type_generics #where_clause {
            type Rust = Self;

            fn encode(value: &Self, hlc: ::zendb_types::Hlc) -> ::zendb_types::Value {
                <<#inner as ::zendb_types::DefaultCrdtCodec>::Codec as ::zendb_types::CrdtCodec>::encode(&value.0, hlc)
            }

            fn decode(value: &::zendb_types::Value) -> Result<Self, ::zendb_types::CellCodecError> {
                Ok(Self(<<#inner as ::zendb_types::DefaultCrdtCodec>::Codec as ::zendb_types::CrdtCodec>::decode(value)?))
            }
        }

        impl #impl_generics ::zendb_types::DefaultCrdtCodec for #ident #type_generics #where_clause {
            type Codec = Self;
        }

        impl #impl_generics ::zendb_types::CellCodecKey for #ident #type_generics #where_clause {
            fn to_primary_key(&self) -> ::zendb_types::PrimaryKey {
                <#inner as ::zendb_types::CellCodecKey>::to_primary_key(&self.0)
            }

            fn from_primary_key(key: &::zendb_types::PrimaryKey) -> Result<Self, ::zendb_types::CellCodecError> {
                Ok(Self(<#inner as ::zendb_types::CellCodecKey>::from_primary_key(key)?))
            }
        }

        impl #impl_generics #ident #type_generics #where_clause {
            pub fn to_cell(&self, hlc: ::zendb_types::Hlc) -> ::zendb_types::Cell {
                <Self as ::zendb_types::CellCodec>::to_cell(self, hlc)
            }

            pub fn from_cell(cell: &::zendb_types::Cell) -> Result<Self, ::zendb_types::CellCodecError> {
                <Self as ::zendb_types::CellCodec>::from_cell(cell)
            }
        }
    })
}

fn derive_enum(
    ident: Ident,
    generics: syn::Generics,
    variants: Vec<syn::Variant>,
) -> Result<proc_macro2::TokenStream> {
    if variants.is_empty() {
        return Err(Error::new_spanned(
            ident,
            "CellCodec unit enums need at least one variant",
        ));
    }
    let mut encoders = Vec::new();
    let mut decoders = Vec::new();
    for variant in variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(Error::new_spanned(
                variant,
                "CellCodec only supports unit enum variants; use a named struct for data-bearing values",
            ));
        }
        if !variant.attrs.is_empty() {
            return Err(Error::new_spanned(
                variant.attrs.first().expect("attribute exists"),
                "CellCodec unit enum variants do not accept attributes",
            ));
        }
        let variant_ident = variant.ident;
        // Variant identifiers are Rust-style PascalCase; the encoded protocol
        // spelling is stable snake_case.
        let stable_name = LitStr::new(
            &snake_case(&variant_ident.to_string()),
            variant_ident.span(),
        );
        encoders.push(quote! { Self::#variant_ident => #stable_name });
        decoders.push(quote! { #stable_name => Ok(Self::#variant_ident) });
    }
    let type_name = LitStr::new(&ident.to_string(), ident.span());
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::zendb_types::CrdtCodec for #ident #type_generics #where_clause {
            type Rust = Self;

            fn encode(value: &Self, _hlc: ::zendb_types::Hlc) -> ::zendb_types::Value {
                let value = match value { #(#encoders,)* };
                ::zendb_types::Value::String(value.into())
            }

            fn decode(value: &::zendb_types::Value) -> Result<Self, ::zendb_types::CellCodecError> {
                let ::zendb_types::Value::String(value) = value else {
                    return Err(::zendb_types::CellCodecError::expected("String"));
                };
                match value.as_str() {
                    #(#decoders,)*
                    _ => Err(::zendb_types::CellCodecError::expected(#type_name)),
                }
            }
        }

        impl #impl_generics ::zendb_types::DefaultCrdtCodec for #ident #type_generics #where_clause {
            type Codec = Self;
        }

        impl #impl_generics ::zendb_types::CellCodecKey for #ident #type_generics #where_clause {
            fn to_primary_key(&self) -> ::zendb_types::PrimaryKey {
                let value = match self { #(#encoders,)* };
                ::zendb_types::PrimaryKey::String(value.into())
            }

            fn from_primary_key(key: &::zendb_types::PrimaryKey) -> Result<Self, ::zendb_types::CellCodecError> {
                let ::zendb_types::PrimaryKey::String(value) = key else {
                    return Err(::zendb_types::CellCodecError::expected("String primary key"));
                };
                match value.as_str() {
                    #(#decoders,)*
                    _ => Err(::zendb_types::CellCodecError::expected(#type_name)),
                }
            }
        }

        impl #impl_generics #ident #type_generics #where_clause {
            pub fn to_cell(&self, hlc: ::zendb_types::Hlc) -> ::zendb_types::Cell {
                <Self as ::zendb_types::CellCodec>::to_cell(self, hlc)
            }

            pub fn from_cell(cell: &::zendb_types::Cell) -> Result<Self, ::zendb_types::CellCodecError> {
                <Self as ::zendb_types::CellCodec>::from_cell(cell)
            }
        }
    })
}

enum CodecSelection {
    Default,
    OrSet,
    Explicit(Box<Type>),
}

fn field_codec(attributes: &[Attribute]) -> Result<CodecSelection> {
    let mut selection = CodecSelection::Default;
    for attribute in attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("cell"))
    {
        attribute.parse_nested_meta(|meta| {
            let value: LitStr = meta.value()?.parse()?;
            if meta.path.is_ident("codec") {
                selection = CodecSelection::Explicit(Box::new(
                    syn::parse_str(&value.value())
                        .map_err(|_| meta.error("CellCodec codec must be a valid Rust type path"))?,
                ));
                return Ok(());
            }
            if meta.path.is_ident("crdt") {
                match value.value().as_str() {
                    "or_set" => selection = CodecSelection::OrSet,
                    "record" => {
                        return Err(meta.error("#[cell(crdt = \"record\")] is not supported"));
                    }
                    _ => {
                        return Err(meta.error(
                            "unsupported CellCodec CRDT; use \"or_set\" or #[cell(codec = \"path::to::Codec\")]",
                        ));
                    }
                }
                return Ok(());
            }
            Err(meta.error(
                "CellCodec supports #[cell(crdt = \"or_set\")] or #[cell(codec = \"path::to::Codec\")]",
            ))
        })?;
    }
    Ok(selection)
}

fn option_inner(ty: &Type) -> (bool, &Type) {
    let Type::Path(path) = ty else {
        return (false, ty);
    };
    let Some(segment) = path.path.segments.last() else {
        return (false, ty);
    };
    if segment.ident != "Option" {
        return (false, ty);
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return (false, ty);
    };
    let Some(GenericArgument::Type(inner)) = arguments.args.first() else {
        return (false, ty);
    };
    (true, inner)
}

fn is_btree_set(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "BTreeSet"))
}

fn codec_type_tokens(selection: CodecSelection, ty: &Type) -> Result<proc_macro2::TokenStream> {
    match selection {
        CodecSelection::Default => Ok(quote! {
            <#ty as ::zendb_types::DefaultCrdtCodec>::Codec
        }),
        CodecSelection::OrSet => {
            let element = btree_set_element(ty).ok_or_else(|| {
                Error::new_spanned(
                    ty,
                    "#[cell(crdt = \"or_set\")] requires a BTreeSet<T> field",
                )
            })?;
            Ok(quote! {
                ::zendb_types::crdt::values::OrSetCodec<#element>
            })
        }
        CodecSelection::Explicit(codec) => Ok(quote! { #codec }),
    }
}

fn btree_set_element(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "BTreeSet" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    match arguments.args.first()? {
        GenericArgument::Type(element) => Some(element),
        _ => None,
    }
}

fn reject_unsupported_type(ty: &Type) -> Result<()> {
    if let Type::Path(path) = ty {
        if let Some(segment) = path.path.segments.last() {
            match segment.ident.to_string().as_str() {
                // A map could be represented by several CRDTs. Reject it
                // rather than silently selecting protocol semantics.
                "BTreeMap" | "HashMap" => {
                    return Err(Error::new_spanned(
                        ty,
                        "CellCodec does not support map fields; maps require an explicit completed record mapping",
                    ));
                }
                "Vec" => {
                    let Some(GenericArgument::Type(Type::Path(element))) =
                        generic_arguments(segment).first()
                    else {
                        return Err(Error::new_spanned(
                            ty,
                            "CellCodec only supports byte vectors (Vec<u8>)",
                        ));
                    };
                    if !element.path.is_ident("u8") {
                        return Err(Error::new_spanned(
                            ty,
                            "CellCodec only supports byte vectors (Vec<u8>)",
                        ));
                    }
                }
                "Option" => {
                    if let Some(GenericArgument::Type(inner)) = generic_arguments(segment).first() {
                        reject_unsupported_type(inner)?;
                    }
                }
                _ => {}
            }
        }
    }
    if let Type::Array(array) = ty {
        if !matches!(array.elem.as_ref(), Type::Path(path) if path.path.is_ident("u8")) {
            return Err(Error::new_spanned(
                ty,
                "CellCodec only supports fixed byte arrays ([u8; N])",
            ));
        }
    }
    Ok(())
}

fn generic_arguments(segment: &syn::PathSegment) -> Vec<&GenericArgument> {
    match &segment.arguments {
        PathArguments::AngleBracketed(arguments) => arguments.args.iter().collect(),
        _ => Vec::new(),
    }
}

fn snake_case(value: &str) -> String {
    let mut result = String::new();
    for (index, character) in value.chars().enumerate() {
        if character.is_ascii_uppercase() {
            if index != 0 {
                result.push('_');
            }
            result.push(character.to_ascii_lowercase());
        } else {
            result.push(character);
        }
    }
    result
}
