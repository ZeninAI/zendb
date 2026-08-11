//! Procedural macros for declaring ZenDB's metadata-owning CRDT types.

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{ToTokens, format_ident, quote};
use syn::{
    FnArg, GenericArgument, ImplItem, ImplItemFn, ItemImpl, ItemStruct, Pat, ReturnType, Type,
    TypePath,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

struct TypeInput {
    item: ItemStruct,
    implementation: ItemImpl,
}

impl Parse for TypeInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let item = input.parse()?;
        let implementation = input.parse()?;
        Ok(Self {
            item,
            implementation,
        })
    }
}

struct Operation {
    method: ImplItemFn,
    variant: Ident,
    args: Vec<(Ident, Type)>,
    output: ReturnType,
    error: Option<Type>,
}

struct EnsureChild {
    method: ImplItemFn,
    segment: Type,
    error: Option<Type>,
}

fn error_key(error: &Type) -> String {
    error.to_token_stream().to_string()
}

fn operation(method: &ImplItemFn, prefix: &str) -> syn::Result<Option<Operation>> {
    let Some(name) = method
        .sig
        .ident
        .to_string()
        .strip_prefix(prefix)
        .map(str::to_owned)
    else {
        return Ok(None);
    };
    if method.sig.inputs.len() < 2 {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "CRDT operation must take &mut self and an EventTime argument",
        ));
    }
    if !matches!(method.sig.inputs.first(), Some(FnArg::Receiver(receiver)) if receiver.reference.is_some() && receiver.mutability.is_some())
    {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "CRDT operation must take &mut self",
        ));
    }
    let Some(FnArg::Typed(remote)) = method.sig.inputs.iter().nth(1) else {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "missing EventTime argument",
        ));
    };
    if !matches!(&*remote.ty, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "EventTime"))
    {
        return Err(syn::Error::new_spanned(
            &remote.ty,
            "the second CRDT method argument must be EventTime",
        ));
    }

    let mut args = Vec::new();
    for input in method.sig.inputs.iter().skip(2) {
        let FnArg::Typed(argument) = input else {
            return Err(syn::Error::new_spanned(
                input,
                "CRDT operation arguments must be named values",
            ));
        };
        let Pat::Ident(pattern) = &*argument.pat else {
            return Err(syn::Error::new_spanned(
                &argument.pat,
                "CRDT operation arguments must be named values",
            ));
        };
        args.push((pattern.ident.clone(), (*argument.ty).clone()));
    }

    let error = match &method.sig.output {
        ReturnType::Type(_, output) => match &**output {
            Type::Path(TypePath { path, .. }) => {
                let Some(last) = path.segments.last() else {
                    return Err(syn::Error::new_spanned(output, "invalid CRDT return type"));
                };
                if last.ident == "bool" {
                    None
                } else if last.ident == "Result" {
                    let syn::PathArguments::AngleBracketed(arguments) = &last.arguments else {
                        return Err(syn::Error::new_spanned(
                            output,
                            "Result must be Result<bool, Error>",
                        ));
                    };
                    let Some(syn::GenericArgument::Type(result)) = arguments.args.first() else {
                        return Err(syn::Error::new_spanned(
                            output,
                            "Result must be Result<bool, Error>",
                        ));
                    };
                    if !matches!(result, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "bool"))
                    {
                        return Err(syn::Error::new_spanned(
                            result,
                            "CRDT operation Result must return bool",
                        ));
                    }
                    let Some(syn::GenericArgument::Type(error)) = arguments.args.iter().nth(1)
                    else {
                        return Err(syn::Error::new_spanned(
                            output,
                            "Result must be Result<bool, Error>",
                        ));
                    };
                    Some(error.clone())
                } else {
                    return Err(syn::Error::new_spanned(
                        output,
                        "CRDT methods must return bool or Result<bool, Error>",
                    ));
                }
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    output,
                    "CRDT methods must return bool or Result<bool, Error>",
                ));
            }
        },
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "CRDT methods must return bool or Result<bool, Error>",
            ));
        }
    };

    let variant = if name.is_empty() {
        format_ident!("Operation")
    } else {
        format_ident!("{}", pascal_case(&name))
    };
    Ok(Some(Operation {
        method: method.clone(),
        variant,
        args,
        output: method.sig.output.clone(),
        error,
    }))
}

fn ensure_child(method: &ImplItemFn) -> syn::Result<Option<EnsureChild>> {
    if method.sig.ident != "ensure_child" {
        return Ok(None);
    }
    if method.sig.inputs.len() != 4 {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "ensure_child must take &mut self, EventTime, a borrowed segment, and TypeTag",
        ));
    }
    if !matches!(method.sig.inputs.first(), Some(FnArg::Receiver(receiver)) if receiver.reference.is_some() && receiver.mutability.is_some())
    {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "ensure_child must take &mut self",
        ));
    }
    let Some(FnArg::Typed(remote)) = method.sig.inputs.iter().nth(1) else {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "ensure_child is missing its EventTime argument",
        ));
    };
    if !matches!(&*remote.ty, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "EventTime"))
    {
        return Err(syn::Error::new_spanned(
            &remote.ty,
            "the second ensure_child argument must be EventTime",
        ));
    }

    let Some(FnArg::Typed(segment)) = method.sig.inputs.iter().nth(2) else {
        unreachable!();
    };
    let Pat::Ident(_) = &*segment.pat else {
        return Err(syn::Error::new_spanned(
            &segment.pat,
            "the ensure_child segment argument must be named",
        ));
    };
    let Type::Reference(reference) = &*segment.ty else {
        return Err(syn::Error::new_spanned(
            &segment.ty,
            "the ensure_child segment argument must be borrowed",
        ));
    };
    if reference.mutability.is_some() {
        return Err(syn::Error::new_spanned(
            &segment.ty,
            "the ensure_child segment argument must be immutably borrowed",
        ));
    }
    let segment_type = (*reference.elem).clone();

    let Some(FnArg::Typed(expected)) = method.sig.inputs.iter().nth(3) else {
        unreachable!();
    };
    let Pat::Ident(_) = &*expected.pat else {
        return Err(syn::Error::new_spanned(
            &expected.pat,
            "the ensure_child TypeTag argument must be named",
        ));
    };
    if !matches!(&*expected.ty, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "TypeTag"))
    {
        return Err(syn::Error::new_spanned(
            &expected.ty,
            "the fourth ensure_child argument must be TypeTag",
        ));
    }

    let error = match &method.sig.output {
        ReturnType::Type(_, output) => match &**output {
            Type::Path(TypePath { path, .. })
                if path
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == "Option") =>
            {
                let last = path
                    .segments
                    .last()
                    .expect("Option path has a final segment");
                let syn::PathArguments::AngleBracketed(arguments) = &last.arguments else {
                    return Err(syn::Error::new_spanned(
                        output,
                        "Option must be Option<&mut Value>",
                    ));
                };
                let Some(GenericArgument::Type(result)) = arguments.args.first() else {
                    return Err(syn::Error::new_spanned(
                        output,
                        "Option must be Option<&mut Value>",
                    ));
                };
                if !matches!(result, Type::Reference(reference) if reference.mutability.is_some() && matches!(&*reference.elem, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "Value")))
                {
                    return Err(syn::Error::new_spanned(
                        result,
                        "ensure_child Option must contain &mut Value",
                    ));
                }
                None
            }
            Type::Path(TypePath { path, .. })
                if path
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == "Result") =>
            {
                let last = path
                    .segments
                    .last()
                    .expect("Result path has a final segment");
                let syn::PathArguments::AngleBracketed(arguments) = &last.arguments else {
                    return Err(syn::Error::new_spanned(
                        output,
                        "Result must be Result<Option<&mut Value>, Error>",
                    ));
                };
                let Some(GenericArgument::Type(result)) = arguments.args.first() else {
                    return Err(syn::Error::new_spanned(
                        output,
                        "Result must be Result<Option<&mut Value>, Error>",
                    ));
                };
                let Type::Path(TypePath {
                    path: option_path, ..
                }) = result
                else {
                    return Err(syn::Error::new_spanned(
                        result,
                        "ensure_child Result must return Option<&mut Value>",
                    ));
                };
                let option = option_path
                    .segments
                    .last()
                    .filter(|segment| segment.ident == "Option")
                    .ok_or_else(|| {
                        syn::Error::new_spanned(
                            result,
                            "ensure_child Result must return Option<&mut Value>",
                        )
                    })?;
                let syn::PathArguments::AngleBracketed(option_arguments) = &option.arguments else {
                    return Err(syn::Error::new_spanned(
                        result,
                        "Option must be Option<&mut Value>",
                    ));
                };
                let Some(GenericArgument::Type(option_value)) = option_arguments.args.first()
                else {
                    return Err(syn::Error::new_spanned(
                        result,
                        "Option must be Option<&mut Value>",
                    ));
                };
                if !matches!(option_value, Type::Reference(reference) if reference.mutability.is_some() && matches!(&*reference.elem, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "Value")))
                {
                    return Err(syn::Error::new_spanned(
                        option_value,
                        "ensure_child Result must contain Option<&mut Value>",
                    ));
                }
                let Some(GenericArgument::Type(error)) = arguments.args.iter().nth(1) else {
                    return Err(syn::Error::new_spanned(
                        output,
                        "Result must be Result<Option<&mut Value>, Error>",
                    ));
                };
                Some(error.clone())
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    output,
                    "ensure_child must return Option<&mut Value> or Result<Option<&mut Value>, Error>",
                ));
            }
        },
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "ensure_child must return Option<&mut Value> or Result<Option<&mut Value>, Error>",
            ));
        }
    };

    Ok(Some(EnsureChild {
        method: method.clone(),
        segment: segment_type,
        error,
    }))
}

fn pascal_case(name: &str) -> String {
    let mut output = String::new();
    for part in name.split('_') {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            output.extend(first.to_uppercase());
            output.extend(chars);
        }
    }
    output
}

fn expand(input: TypeInput, container: bool) -> syn::Result<proc_macro2::TokenStream> {
    let TypeInput {
        mut item,
        implementation,
    } = input;
    let name = item.ident.clone();
    let op_name = format_ident!("{}Op", name);

    let fields = match &mut item.fields {
        syn::Fields::Named(fields) => &mut fields.named,
        _ => {
            return Err(syn::Error::new_spanned(
                &item,
                "ZenDB types must be named structs",
            ));
        }
    };
    fields.push(syn::parse_quote! {
        #[doc(hidden)]
        pub(crate) __event_time: ::zendb_types::EventTime
    });
    fields.push(syn::parse_quote! {
        #[doc(hidden)]
        pub(crate) __is_tombstone: bool
    });

    let mut operations = Vec::new();
    let mut ensure_children = Vec::new();
    for item in &implementation.items {
        let ImplItem::Fn(method) = item else { continue };
        if let Some(operation) = operation(method, "op_")? {
            operations.push(operation);
        }
        if let Some(ensure_child) = ensure_child(method)? {
            ensure_children.push(ensure_child);
        }
    }
    if operations.is_empty() {
        return Err(syn::Error::new_spanned(
            &implementation,
            "a ZenDB type must define at least one op_* method",
        ));
    }
    let error_name = format_ident!("{}OpError", name);
    let mut error_variants: Vec<(String, Ident, Type)> = Vec::new();
    for error in operations
        .iter()
        .filter_map(|operation| operation.error.as_ref())
        .chain(
            ensure_children
                .iter()
                .filter_map(|ensure_child| ensure_child.error.as_ref()),
        )
    {
        let key = error_key(error);
        if !error_variants.iter().any(|(known, _, _)| *known == key) {
            let variant = if ensure_children.iter().any(|ensure_child| {
                ensure_child
                    .error
                    .as_ref()
                    .is_some_and(|known| error_key(known) == key)
            }) {
                format_ident!("EnsureChild")
            } else {
                operations
                    .iter()
                    .find(|operation| {
                        operation
                            .error
                            .as_ref()
                            .is_some_and(|known| error_key(known) == key)
                    })
                    .expect("operation error variant was registered")
                    .variant
                    .clone()
            };
            error_variants.push((key, variant, error.clone()));
        }
    }

    let error_definition = if error_variants.is_empty() {
        quote! {}
    } else {
        let error_enum_variants = error_variants.iter().map(|(_, variant, error)| {
            quote! { #variant(#error) }
        });
        let error_display_arms = error_variants.iter().map(|(_, variant, _)| {
            quote! { Self::#variant(error) => write!(formatter, "{}({error})", stringify!(#variant)), }
        });
        let error_source_arms = error_variants.iter().map(|(_, variant, _)| {
            quote! { Self::#variant(error) => Some(error), }
        });
        quote! {
            #[derive(Debug)]
            pub enum #error_name {
                #(#error_enum_variants),*
            }

            impl std::fmt::Display for #error_name {
                fn fmt(
                    &self,
                    formatter: &mut std::fmt::Formatter<'_>,
                ) -> std::fmt::Result {
                    match self {
                        #(#error_display_arms)*
                    }
                }
            }

            impl std::error::Error for #error_name {
                fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                    match self {
                        #(#error_source_arms)*
                    }
                }
            }
        }
    };
    let type_error = if error_variants.is_empty() {
        quote! { ::std::convert::Infallible }
    } else {
        quote! { #error_name }
    };

    let variants = operations.iter().map(|operation| {
        let variant = &operation.variant;
        let fields = operation
            .args
            .iter()
            .map(|(name, ty)| quote! { #name: #ty });
        quote! { #variant { #(#fields),* } }
    });
    let dispatch = operations.iter().map(|operation| {
        let variant = &operation.variant;
        let method = &operation.method.sig.ident;
        let names: Vec<_> = operation
            .args
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let call = quote! { self.#method(remote, #(#names.clone()),*) };
        let result = match &operation.error {
            Some(error) => {
                let error_variant = error_variants
                    .iter()
                    .find(|(key, _, _)| *key == error_key(error))
                    .map(|(_, variant, _)| variant)
                    .expect("operation error variant was registered");
                quote! { #call.map_err(#error_name::#error_variant) }
            }
            None => quote! { Ok(#call) },
        };
        quote! {
            Self::Op::#variant { #(#names),* } => #result,
        }
    });
    let facades = operations.iter().map(|operation| {
        let method_name = &operation.method.sig.ident;
        let facade_name = format_ident!("{}", method_name.to_string().trim_start_matches("op_"));
        let args = operation
            .args
            .iter()
            .map(|(name, ty)| quote! { #name: #ty });
        let names: Vec<_> = operation
            .args
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let output = &operation.output;
        quote! {
            pub fn #facade_name(&mut self, #(#args),*) #output {
                let remote = ::zendb_types::global_clock().mint();
                self.#method_name(remote, #(#names),*)
            }
        }
    });
    let container_impl = if container {
        if ensure_children.len() != 1 {
            return Err(syn::Error::new_spanned(
                &implementation,
                "a container type must define exactly one ensure_child method",
            ));
        }
        let ensure_child = &ensure_children[0];
        let method = &ensure_child.method.sig.ident;
        let result = match &ensure_child.error {
            Some(error) => {
                let error_variant = error_variants
                    .iter()
                    .find(|(key, _, _)| *key == error_key(error))
                    .map(|(_, variant, _)| variant)
                    .expect("ensure_child error variant was registered");
                quote! { #name::#method(self, remote, segment, expected).map_err(#error_name::#error_variant) }
            }
            None => quote! { Ok(#name::#method(self, remote, segment, expected)) },
        };
        let segment_type = &ensure_child.segment;
        quote! {
            impl ::zendb_types::ContainerType for #name {
                type Segment = #segment_type;

                fn ensure_child(
                    &mut self,
                    remote: ::zendb_types::EventTime,
                    segment: &Self::Segment,
                    expected: ::zendb_types::TypeTag,
                ) -> Result<Option<&mut ::zendb_types::Value>, Self::Error> {
                    #result
                }

            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        #item
        #implementation

        #error_definition

        #[derive(Debug, Clone, ::bincode::Encode, ::bincode::Decode)]
        pub enum #op_name {
            #(#variants),*
        }

        impl #name {
            #(#facades)*
        }

        impl ::zendb_types::Type for #name {
            type Op = #op_name;
            type Error = #type_error;

            fn apply(
                &mut self,
                remote: ::zendb_types::EventTime,
                op: &Self::Op,
            ) -> Result<bool, Self::Error> {
                match op {
                    #(#dispatch)*
                }
            }

            fn event_time(&self) -> ::zendb_types::EventTime {
                self.__event_time
            }

            fn set_event_time(&mut self, time: ::zendb_types::EventTime) {
                self.__event_time = time;
            }

            fn is_tombstone(&self) -> bool {
                self.__is_tombstone
            }

            fn set_tombstone(&mut self, tombstone: bool) {
                self.__is_tombstone = tombstone;
            }
        }

        #container_impl
    })
}

/// Declare a metadata-owning leaf CRDT type and generate its operation enum and facades.
#[proc_macro]
pub fn zendb_type(input: TokenStream) -> TokenStream {
    match expand(parse_macro_input!(input as TypeInput), false) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

/// Declare a metadata-owning container CRDT type and generate child dispatch in addition to operations.
#[proc_macro]
pub fn zendb_container_type(input: TokenStream) -> TokenStream {
    match expand(parse_macro_input!(input as TypeInput), true) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}
