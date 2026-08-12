use crate::{Edit, Record, Segment, String, Text, TextError, TextOp, TextOpError, TypeOp, Value};

#[test]
fn edit_records_a_nested_record_path() {
    let mut edit = Edit::empty();

    assert!(
        edit.typed::<Record>()
            .record("profile")
            .typed::<Record>()
            .record("name")
            .typed::<String>()
            .set("Ada".to_owned())
            .expect("string edit should apply")
    );

    assert_eq!(edit.changes().len(), 1);
    let change = &edit.changes()[0];
    assert_eq!(
        change.path,
        vec![
            Segment::Record("profile".to_owned()),
            Segment::Record("name".to_owned()),
        ]
    );
    assert!(matches!(
        &change.op,
        TypeOp::String(crate::StringOp::Set { value }) if value == "Ada"
    ));

    let Value::Record(root) = edit.value().expect("edit should contain a root value") else {
        panic!("expected a record root");
    };
    let Some(Value::Record(profile)) = root.get("profile") else {
        panic!("expected a profile record");
    };
    let Some(Value::String(name)) = profile.get("name") else {
        panic!("expected a string name");
    };
    assert_eq!(name.value, "Ada");

    let (value, changes) = edit.into_parts();
    assert!(value.is_some());
    assert_eq!(changes.len(), 1);
}

#[test]
fn text_builder_returns_an_op_and_edit_applies_it_separately() {
    let text = Text::default();
    let op = text
        .build_insert_at(0, "Ada".to_owned())
        .expect("position zero should be valid");

    assert!(matches!(
        &op,
        TextOp::Insert { after: None, text } if text == "Ada"
    ));

    let mut edit = Edit::new(text);
    assert!(
        edit.typed::<Text>()
            .apply(op)
            .expect("text operation should apply")
    );

    let Value::Text(text) = edit.value().expect("edit should contain text") else {
        panic!("expected a text value");
    };
    assert_eq!(text.string(), "Ada");
    assert!(matches!(
        &edit.changes()[0].op,
        TypeOp::Text(TextOp::Insert { after: None, text }) if text == "Ada"
    ));

    assert!(matches!(
        Text::default().build_insert_at(1, "invalid".to_owned()),
        Err(TextError::InvalidPosition)
    ));

    let mut text = Text::default();
    assert!(
        text.insert_at(0, "direct".to_owned())
            .expect("generated applying facade should succeed")
    );
    assert_eq!(text.string(), "direct");

    let error = Text::default()
        .insert_at(1, "invalid".to_owned())
        .expect_err("generated applying facade should report builder errors");
    assert!(matches!(
        error,
        TextOpError::Insert(TextError::InvalidPosition)
    ));
}
