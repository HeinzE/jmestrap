//! JMESPath predicate types for filtering and completion conditions
//!
//! - `Jmes` - single predicate for continuous matching
//! - `JmesUntil` - completion conditions (Order, AnyOrder)
//!
//! Thread-safety: jmespath::Expression uses Rc internally (not Send/Sync).
//! We solve this with a thread-local cache: expressions are compiled once
//! per thread and reused for subsequent evaluations.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::HashMap;

// =============================================================================
// Thread-local JMESPath expression cache
// =============================================================================

thread_local! {
    /// Cache of compiled JMESPath expressions per thread.
    /// Key is the decorated expression string "[?predicate]".
    /// Each tokio worker thread maintains its own cache.
    static EXPR_CACHE: RefCell<HashMap<String, jmespath::Expression<'static>>>
        = RefCell::new(HashMap::new());
}

/// Evaluate a JMESPath predicate (wrapped as filter projection) against an event.
/// Uses thread-local cache for compiled expressions.
fn evaluate_predicate(predicate: &str, event: &JsonValue) -> bool {
    let decorated = format!("[?{}]", predicate);

    EXPR_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();

        // Get or compile the expression
        let compiled = match cache.entry(decorated.clone()) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => match jmespath::compile(&decorated) {
                Ok(expr) => e.insert(expr),
                Err(_) => return false, // should not happen if validated, but never panic
            },
        };

        // Wrap event in array for filter projection
        let wrapped = JsonValue::Array(vec![event.clone()]);

        match compiled.search(&wrapped) {
            Ok(result) => {
                // Filter projection returns non-empty array if matched
                if let Some(arr) = result.as_array() {
                    !arr.is_empty()
                } else {
                    false
                }
            }
            Err(_) => false,
        }
    })
}

/// Validate a JMESPath predicate at compile time (and warm the cache)
fn validate_predicate(predicate: &str) -> Result<(), String> {
    let decorated = format!("[?{}]", predicate);
    let compiled =
        jmespath::compile(&decorated).map_err(|e| format!("JMESPath compile error: {}", e))?;

    // Warm the cache on this thread
    EXPR_CACHE.with(|cache| {
        cache.borrow_mut().insert(decorated, compiled);
    });

    Ok(())
}

// =============================================================================
// Jmes - Single predicate for continuous matching
// =============================================================================

/// Single JMESPath predicate for filtering events to record
/// Thread-safe: stores expression string, compiles on each evaluation
#[derive(Debug, Clone)]
pub struct Jmes {
    expression: String,
}

impl Jmes {
    /// Create a new Jmes predicate.
    /// The predicate is wrapped in a filter projection `[?{predicate}]`.
    pub fn new(predicate: &str) -> Result<Self, String> {
        validate_predicate(predicate)?;
        Ok(Self {
            expression: predicate.to_string(),
        })
    }

    /// Check if an event matches this predicate.
    /// `@` (match everything) is short-circuited without JMESPath evaluation.
    pub fn is_match(&self, event: &JsonValue) -> bool {
        if self.expression == "@" {
            return true;
        }
        evaluate_predicate(&self.expression, event)
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }
}

// =============================================================================
// JmesUntil - Completion conditions
// =============================================================================

/// A single predicate within an Until condition
/// Thread-safe: stores expression string, compiles on each evaluation
#[derive(Debug, Clone)]
pub struct JmesPredicate {
    expression: String,
    pub matched: bool,
}

impl JmesPredicate {
    fn new(predicate: &str) -> Result<Self, String> {
        validate_predicate(predicate)?;
        Ok(Self {
            expression: predicate.to_string(),
            matched: false,
        })
    }

    fn is_match(&self, event: &JsonValue) -> bool {
        evaluate_predicate(&self.expression, event)
    }
}

/// Until condition variants - determines when a recording finishes
#[derive(Debug)]
pub enum JmesUntil {
    /// Predicates must match in sequence
    Order(Vec<JmesPredicate>),
    /// All predicates must match, in any order
    AnyOrder(Vec<JmesPredicate>),
}

impl JmesUntil {
    /// Create from a serializable UntilSpec
    pub fn from_spec(spec: UntilSpec) -> Result<Self, String> {
        spec.compile()
    }

    /// Create an Order (sequential) until condition
    pub fn order(predicates: &[&str]) -> Result<Self, String> {
        let preds = predicates
            .iter()
            .map(|p| JmesPredicate::new(p))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::Order(preds))
    }

    /// Create an AnyOrder (unordered) until condition
    pub fn any_order(predicates: &[&str]) -> Result<Self, String> {
        let preds = predicates
            .iter()
            .map(|p| JmesPredicate::new(p))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::AnyOrder(preds))
    }

    /// Try to match an event against the until condition
    /// Returns Some(predicate_index) if matched, None otherwise
    /// Index is 1-based
    pub fn try_match(&mut self, event: &JsonValue) -> Option<usize> {
        match self {
            JmesUntil::Order(predicates) => {
                // Find first unmatched predicate
                if let Some((idx, pred)) =
                    predicates.iter_mut().enumerate().find(|(_, p)| !p.matched)
                {
                    if pred.is_match(event) {
                        pred.matched = true;
                        return Some(idx + 1); // 1-based index
                    }
                }
                None
            }
            JmesUntil::AnyOrder(predicates) => {
                // Try to match any unmatched predicate
                for (idx, pred) in predicates.iter_mut().enumerate() {
                    if !pred.matched && pred.is_match(event) {
                        pred.matched = true;
                        return Some(idx + 1); // 1-based index
                    }
                }
                None
            }
        }
    }

    /// Check if all predicates have been matched
    pub fn is_complete(&self) -> bool {
        match self {
            JmesUntil::Order(predicates) | JmesUntil::AnyOrder(predicates) => {
                predicates.iter().all(|p| p.matched)
            }
        }
    }

    /// Get the match status of each predicate
    pub fn matched_status(&self) -> Vec<bool> {
        match self {
            JmesUntil::Order(predicates) | JmesUntil::AnyOrder(predicates) => {
                predicates.iter().map(|p| p.matched).collect()
            }
        }
    }

    /// Get predicate expressions
    pub fn expressions(&self) -> Vec<&str> {
        match self {
            JmesUntil::Order(predicates) | JmesUntil::AnyOrder(predicates) => {
                predicates.iter().map(|p| p.expression.as_str()).collect()
            }
        }
    }

    /// Reset all predicates to unmatched state
    #[allow(dead_code)]
    pub fn reset(&mut self) {
        match self {
            JmesUntil::Order(predicates) | JmesUntil::AnyOrder(predicates) => {
                for pred in predicates.iter_mut() {
                    pred.matched = false;
                }
            }
        }
    }
}

// =============================================================================
// Serialization for commands
// =============================================================================

/// Serializable representation of an until condition
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UntilSpec {
    Order { predicates: Vec<String> },
    AnyOrder { predicates: Vec<String> },
}

impl UntilSpec {
    pub fn compile(self) -> Result<JmesUntil, String> {
        match self {
            UntilSpec::Order { predicates } => {
                let refs: Vec<&str> = predicates.iter().map(|s| s.as_str()).collect();
                JmesUntil::order(&refs)
            }
            UntilSpec::AnyOrder { predicates } => {
                let refs: Vec<&str> = predicates.iter().map(|s| s.as_str()).collect();
                JmesUntil::any_order(&refs)
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_jmes_simple_match() {
        let jmes = Jmes::new("type == 'temperature'").unwrap();

        let event1 = json!({"type": "temperature", "value": 23.5});
        let event2 = json!({"type": "humidity", "value": 65});

        assert!(jmes.is_match(&event1));
        assert!(!jmes.is_match(&event2));
    }

    #[test]
    fn test_jmes_nested_match() {
        let jmes = Jmes::new("data.sensor == 'temp1'").unwrap();

        let event1 = json!({"data": {"sensor": "temp1", "value": 23.5}});
        let event2 = json!({"data": {"sensor": "temp2", "value": 24.0}});

        assert!(jmes.is_match(&event1));
        assert!(!jmes.is_match(&event2));
    }

    #[test]
    fn test_until_order() {
        let mut until =
            JmesUntil::order(&["state == 'init'", "state == 'ready'", "state == 'done'"]).unwrap();

        // Must match in order
        assert!(!until.is_complete());

        // Skip to ready - doesn't match (init not yet matched)
        let ready = json!({"state": "ready"});
        assert!(until.try_match(&ready).is_none());

        // Match init
        let init = json!({"state": "init"});
        assert_eq!(until.try_match(&init), Some(1));

        // Now ready matches
        assert_eq!(until.try_match(&ready), Some(2));

        // Match done
        let done = json!({"state": "done"});
        assert_eq!(until.try_match(&done), Some(3));

        assert!(until.is_complete());
    }

    #[test]
    fn test_until_any_order() {
        let mut until =
            JmesUntil::any_order(&["type == 'a'", "type == 'b'", "type == 'c'"]).unwrap();

        // Can match in any order
        let b = json!({"type": "b"});
        let a = json!({"type": "a"});
        let c = json!({"type": "c"});

        assert_eq!(until.try_match(&b), Some(2));
        assert!(!until.is_complete());

        assert_eq!(until.try_match(&c), Some(3));
        assert!(!until.is_complete());

        assert_eq!(until.try_match(&a), Some(1));
        assert!(until.is_complete());
    }

    #[test]
    fn test_until_spec_deserialize() {
        let json = r#"{"type": "order", "predicates": ["state == 'a'", "state == 'b'"]}"#;
        let spec: UntilSpec = serde_json::from_str(json).unwrap();
        let until = spec.compile().unwrap();

        assert!(!until.is_complete());
    }

    // =========================================================================
    // Tests moved to here that were initially external
    // =========================================================================

    #[test]
    fn test_jmes_syntax_errors() {
        //
        let bad_predicates = [
            "event == 'unbalanced",               // unclosed delimiter
            "state == `incomplete",               // unclosed literal
            "99",                                 // bare number
            "statusCode == 404suffix",            // invalid token
            "status == ",                         // trailing operator
            "state == 'incomplete' && value=`9`", // single = instead of ==
            "state == 'incomplete' & value==`9`", // single & instead of &&
        ];

        for pred in &bad_predicates {
            assert!(
                Jmes::new(pred).is_err(),
                "Expected error for predicate: {:?}",
                pred
            );
        }
    }

    #[test]
    fn test_jmes_order_with_non_matching_interleaved() {
        //
        let mut until =
            JmesUntil::order(&["event == 'start'", "event == 'update'", "event == 'stop'"])
                .unwrap();

        assert_eq!(until.try_match(&json!({"event": "start"})), Some(1));
        assert!(until.try_match(&json!({"event": "restart"})).is_none()); // non-match ignored
        assert_eq!(until.try_match(&json!({"event": "update"})), Some(2));
        assert_eq!(until.try_match(&json!({"event": "stop"})), Some(3));
        assert!(until.is_complete());
        // After completion, further events don't match
        assert!(until.try_match(&json!({"event": "restart"})).is_none());
    }

    #[test]
    fn test_jmes_any_order_with_non_matching_interleaved() {
        //
        let mut until =
            JmesUntil::any_order(&["event == 'start'", "event == 'update'", "event == 'finish'"])
                .unwrap();

        assert!(until.try_match(&json!({"event": "other"})).is_none());

        assert_eq!(until.try_match(&json!({"event": "update"})), Some(2));
        assert!(until.try_match(&json!({"event": "other"})).is_none());

        assert_eq!(until.try_match(&json!({"event": "start"})), Some(1));
        assert!(!until.is_complete());

        assert_eq!(until.try_match(&json!({"event": "finish"})), Some(3));
        assert!(until.is_complete());
    }

    #[test]
    fn test_jmes_flag_presence_and_truthiness() {
        //
        let event_flag_true = json!({"flag": true});
        let event_flag_false = json!({"flag": false});
        let event_no_flag = json!({});

        // contains(keys(@), 'flag') — presence check
        let presence = Jmes::new("contains(keys(@), 'flag')").unwrap();
        assert!(presence.is_match(&event_flag_true));
        assert!(presence.is_match(&event_flag_false));
        assert!(!presence.is_match(&event_no_flag));

        // flag — truthiness
        let truthy = Jmes::new("flag").unwrap();
        assert!(truthy.is_match(&event_flag_true));
        assert!(!truthy.is_match(&event_flag_false));
        assert!(!truthy.is_match(&event_no_flag));

        // flag && flag==`true` — explicit true check
        let truthy_and_true = Jmes::new("flag && flag==`true`").unwrap();
        assert!(truthy_and_true.is_match(&event_flag_true));
        assert!(!truthy_and_true.is_match(&event_flag_false));
        assert!(!truthy_and_true.is_match(&event_no_flag));
    }

    #[test]
    fn test_until_spec_deserialize_any_order() {
        let json = r#"{"type": "any_order", "predicates": ["type == 'a'", "type == 'b'"]}"#;
        let spec: UntilSpec = serde_json::from_str(json).unwrap();
        let mut until = spec.compile().unwrap();

        // Verify it works as any_order
        assert_eq!(until.try_match(&json!({"type": "b"})), Some(2));
        assert_eq!(until.try_match(&json!({"type": "a"})), Some(1));
        assert!(until.is_complete());
    }

    #[test]
    fn test_until_spec_deserialize_missing_type() {
        //
        let result = serde_json::from_str::<UntilSpec>(r#"{"predicates": ["event == 'start'"]}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_until_spec_deserialize_unknown_type() {
        //
        let result = serde_json::from_str::<UntilSpec>(
            r#"{"type": "unknown", "predicates": ["event == 'start'"]}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_jmes_invalid_predicate_in_until() {
        // Compile-time validation of predicates inside Until
        assert!(JmesUntil::order(&["event == 'ok'", "bad predicate [["]).is_err());
        assert!(JmesUntil::any_order(&["bad predicate [["]).is_err());
    }
}
