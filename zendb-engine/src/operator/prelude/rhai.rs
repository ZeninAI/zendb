use std::io;

use bincode::{Decode, Encode};
use rhai::{Array, Dynamic, Engine, Map, Scope};

use crate::{BoxFuture, Change, DispatchOperator, Operator, OperatorContext, OperatorDirective};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RhaiOperatorConfig {
    pub script: String,
}

pub struct RhaiOperator;

impl Operator for RhaiOperator {
    type Config = RhaiOperatorConfig;
    type Timer = ();

    fn new(_config: &Self::Config) -> io::Result<Self> {
        Ok(Self)
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        let result = run_script(ctx.name(), ctx.config(), changes);
        Box::pin(async move { result })
    }
}

fn run_script(
    name: &str,
    config: &RhaiOperatorConfig,
    changes: Vec<Change>,
) -> io::Result<OperatorDirective> {
    let engine = Engine::new();
    let ast = engine
        .compile(&config.script)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;

    let mut scope = Scope::new();
    scope.push("name", name.to_owned());
    scope.push("changes", changes_to_array(changes));

    let output: Dynamic = engine
        .eval_ast_with_scope(&mut scope, &ast)
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))?;

    directive_from_dynamic(output)
}

fn directive_from_dynamic(output: Dynamic) -> io::Result<OperatorDirective> {
    if output.is_unit() {
        return Ok(OperatorDirective::Continue);
    }
    if let Some(done) = output.clone().try_cast::<bool>() {
        return Ok(if done {
            OperatorDirective::Finish
        } else {
            OperatorDirective::Continue
        });
    }
    if let Some(value) = output.clone().try_cast::<String>() {
        return directive_from_str(&value);
    }
    if let Some(map) = output.try_cast::<Map>() {
        if let Some(value) = map
            .get("directive")
            .and_then(|value| value.clone().try_cast())
        {
            return directive_from_str(value);
        }
    }

    Ok(OperatorDirective::Continue)
}

fn directive_from_str(value: &str) -> io::Result<OperatorDirective> {
    match value {
        "continue" | "Continue" => Ok(OperatorDirective::Continue),
        "finish" | "Finish" => Ok(OperatorDirective::Finish),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown Rhai operator directive {value:?}"),
        )),
    }
}

fn changes_to_array(changes: Vec<Change>) -> Array {
    changes
        .into_iter()
        .map(|change| Dynamic::from(change_to_map(change)))
        .collect()
}

fn change_to_map(change: Change) -> Map {
    let mut map = Map::new();
    map.insert("table".into(), change.event.table_id.into());
    map.insert(
        "primary_key".into(),
        format!("{:?}", change.event.primary_key).into(),
    );
    map.insert("path".into(), format!("{:?}", change.event.path).into());
    map.insert("op".into(), format!("{:?}", change.event.op).into());
    map.insert(
        "hlc_ms".into(),
        (change.event.hlc.physical_ms() as i64).into(),
    );
    map.insert(
        "hlc_logical".into(),
        (change.event.hlc.logical() as i64).into(),
    );
    map.insert("sync".into(), change.event.sync.into());
    map.insert(
        "previous".into(),
        change
            .previous
            .map(|cell| format!("{cell:?}"))
            .unwrap_or_default()
            .into(),
    );
    map.insert(
        "current".into(),
        change
            .current
            .map(|cell| format!("{cell:?}"))
            .unwrap_or_default()
            .into(),
    );
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_can_finish_operator() {
        let config = RhaiOperatorConfig {
            script: r#""finish""#.to_owned(),
        };

        assert_eq!(
            run_script("scripted", &config, Vec::new()).unwrap(),
            OperatorDirective::Finish
        );
    }

    #[test]
    fn script_can_read_change_batch() {
        let config = RhaiOperatorConfig {
            script: r#"if changes.len() == 0 { "continue" } else { "finish" }"#.to_owned(),
        };

        assert_eq!(
            run_script("scripted", &config, Vec::new()).unwrap(),
            OperatorDirective::Continue
        );
    }
}
