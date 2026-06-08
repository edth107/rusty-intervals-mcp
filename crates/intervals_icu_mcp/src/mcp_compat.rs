use std::sync::Arc;

use rmcp::model::Tool;
use serde_json::{Value, json};

pub fn sanitize_tools(tools: Vec<Tool>) -> Vec<Tool> {
    tools.into_iter().map(sanitize_tool).collect()
}

pub fn sanitize_tool(mut tool: Tool) -> Tool {
    tool.output_schema = None;

    let mut input_schema = Value::Object(tool.input_schema.as_ref().clone());
    fix_schema(&mut input_schema);
    if let Value::Object(schema) = input_schema {
        tool.input_schema = Arc::new(schema);
    }

    tool
}

fn fix_schema(value: &mut Value) {
    match value {
        Value::Object(obj) => {
            for (key, val) in obj.iter_mut() {
                if val == &Value::Bool(true) && key != "required" {
                    *val = json!({
                        "type": "object",
                        "description": key
                    });
                } else {
                    fix_schema(val);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                fix_schema(item);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use rmcp::model::Tool;
    use serde_json::{Map, Value, json};

    use super::sanitize_tool;

    #[test]
    fn sanitize_tool_removes_output_schema_and_replaces_bare_true_schema_nodes() {
        let input_schema = json!({
            "type": "object",
            "properties": {
                "gear": true,
                "required": true,
                "nested": { "value": true }
            }
        });
        let output_schema = json!({
            "type": "object",
            "properties": { "value": true }
        });

        let tool = Tool {
            name: Cow::Borrowed("example"),
            title: None,
            description: None,
            input_schema: std::sync::Arc::new(input_schema.as_object().unwrap().clone()),
            output_schema: Some(std::sync::Arc::new(
                output_schema.as_object().unwrap().clone(),
            )),
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        };

        let sanitized = sanitize_tool(tool);
        assert!(sanitized.output_schema.is_none());

        let schema = Value::Object(Map::clone(sanitized.input_schema.as_ref()));
        assert_eq!(schema["properties"]["gear"]["type"], "object");
        assert_eq!(schema["properties"]["gear"]["description"], "gear");
        assert_eq!(schema["properties"]["nested"]["value"]["type"], "object");
        assert_eq!(schema["properties"]["required"], true);
    }
}
