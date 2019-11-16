// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::Json;

impl Json {
    pub fn depth(&self) -> i64 {
        match &self {
            Json::Object(_) | Json::Array(_) => depth_json(self),
            _ => 1,
        }
    }
}

pub fn depth_json(j: &Json) -> i64 {
    (match *j {
        Json::Array(ref array) => array
            .iter()
            .map(|child| depth_json(child))
            .max()
            .unwrap_or(0),

        Json::Object(ref map) => map
            .iter()
            .map(|(_, value)| depth_json(value))
            .max()
            .unwrap_or(0),
        _ => 0,
    } + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_depth() {
        let mut test_cases =
            vec![
            ("null", 1),
            ("[true, 2017]", 2),
            (r#"{"a": {"a1": [3]}, "b": {"b1": {"c": {"d": [5]}}}}"#, 6),
            ("{}", 1),
            ("[]", 1),
            ("true", 1),
            ("1", 1),
            ("-1", 1),
            (r#""a""#, 1),
            (r#"[10, 20]"#, 2),
            (r#"[[], {}]"#, 2),
            (r#"[10, {"a": 20}]"#, 3),
            (r#"[[2], 3, [[[4]]]]"#, 5),
            (r#"{"Name": "Homer"}"#, 2),
            (r#"[10, {"a": 20}]"#, 3),
            (r#"{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }"#, 4),
            (r#"{"a":1}"#, 2),
            (r#"{"a":[1]}"#, 3),
            (r#"{"b":2, "c":3}"#, 2),
            (r#"[1]"#, 2),
            (r#"[1,2]"#, 2),
            (r#"[1,2,[1,3]]"#, 3),
            (r#"[1,2,[1,[5,[3]]]]"#, 5),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, 6),
            (r#"[{"a":1}]"#, 3),
            (r#"[{"a":1,"b":2}]"#, 3),
            (r#"[{"a":{"a":1},"b":2}]"#, 4),
        ];
        for (i, (js, expected)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let got = j.depth();
            assert_eq!(
                got, expected,
                "#{} expect {:?}, but got {:?}",
                i, expected, got
            );
        }
    }
}
