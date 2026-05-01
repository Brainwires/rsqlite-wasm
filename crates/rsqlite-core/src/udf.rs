//! User-defined scalar function registry.
//!
//! Functions registered here are looked up by [`crate::eval_helpers::eval_scalar_function`]
//! after every built-in dispatch misses, so a UDF can shadow a missing
//! built-in but cannot accidentally override one.
//!
//! The registry is per-thread (`thread_local!`) — same pattern used by
//! bound parameters and other executor-side state. Storing callbacks
//! per-thread sidesteps the `Send` requirements that would otherwise
//! complicate registering JavaScript closures from `wasm-bindgen`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

use crate::error::Result;

/// Callback signature for a UDF: receives the evaluated arguments, returns
/// the resulting value (or an error that propagates out of the query).
pub type UdfCallback = Rc<dyn Fn(&[Value]) -> Result<Value>>;

/// Metadata for a registered UDF.
pub struct UdfEntry {
    /// Number of arguments the function expects, or `None` for variadic.
    pub n_args: Option<usize>,
    pub callback: UdfCallback,
}

thread_local! {
    static UDF_REGISTRY: RefCell<HashMap<String, UdfEntry>> = RefCell::new(HashMap::new());
}

/// Register a user-defined function. Names are case-insensitive (stored
/// upper-cased to match the built-in dispatch). Re-registering the same
/// name replaces the previous callback.
pub fn register(name: &str, n_args: Option<usize>, callback: UdfCallback) {
    let key = name.to_ascii_uppercase();
    UDF_REGISTRY.with(|r| {
        r.borrow_mut().insert(key, UdfEntry { n_args, callback });
    });
}

/// Remove a previously-registered function. Returns `true` if a function by
/// that name was removed.
pub fn unregister(name: &str) -> bool {
    let key = name.to_ascii_uppercase();
    UDF_REGISTRY.with(|r| r.borrow_mut().remove(&key).is_some())
}

/// Drop every registered function. Useful for tests that need a clean slate.
pub fn clear() {
    UDF_REGISTRY.with(|r| r.borrow_mut().clear());
}

/// Check whether a function is registered. Used by the planner before
/// rejecting an unknown function name.
pub fn is_registered(name: &str) -> bool {
    let key = name.to_ascii_uppercase();
    UDF_REGISTRY.with(|r| r.borrow().contains_key(&key))
}

/// Look up and invoke a UDF by name. Returns `None` if no UDF is registered
/// under `name` (so the caller can fall through to its "unknown function"
/// error). The arity check matches SQLite's behavior: a UDF declared with a
/// fixed `n_args` errors when called with the wrong number of arguments.
pub fn invoke(name: &str, args: &[Value]) -> Option<Result<Value>> {
    let key = name.to_ascii_uppercase();
    UDF_REGISTRY.with(|r| {
        let registry = r.borrow();
        let entry = registry.get(&key)?;
        if let Some(expected) = entry.n_args {
            if args.len() != expected {
                return Some(Err(crate::error::Error::Other(format!(
                    "user function {name} expected {expected} arguments, got {}",
                    args.len()
                ))));
            }
        }
        let cb = entry.callback.clone();
        drop(registry);
        Some(cb(args))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn i(n: i64) -> Value {
        Value::Integer(n)
    }

    #[test]
    fn register_and_invoke_fixed_arity() {
        clear();
        register(
            "double",
            Some(1),
            Rc::new(|args: &[Value]| match &args[0] {
                Value::Integer(n) => Ok(Value::Integer(n * 2)),
                _ => Err(crate::error::Error::Other("expected integer".into())),
            }),
        );
        let result = invoke("DOUBLE", &[i(21)]).unwrap().unwrap();
        assert_eq!(result, i(42));
        // Case insensitive on lookup.
        let result = invoke("double", &[i(5)]).unwrap().unwrap();
        assert_eq!(result, i(10));
        clear();
    }

    #[test]
    fn arity_mismatch_errors() {
        clear();
        register(
            "needs_two",
            Some(2),
            Rc::new(|args: &[Value]| Ok(args[0].clone())),
        );
        let result = invoke("needs_two", &[i(1)]).unwrap();
        assert!(result.is_err());
        clear();
    }

    #[test]
    fn variadic_function_accepts_any_count() {
        clear();
        register(
            "first_arg",
            None,
            Rc::new(|args: &[Value]| {
                Ok(args.first().cloned().unwrap_or(Value::Null))
            }),
        );
        assert_eq!(invoke("first_arg", &[]).unwrap().unwrap(), Value::Null);
        assert_eq!(invoke("first_arg", &[i(7)]).unwrap().unwrap(), i(7));
        assert_eq!(invoke("first_arg", &[i(1), i(2)]).unwrap().unwrap(), i(1));
        clear();
    }

    #[test]
    fn unregister_removes() {
        clear();
        register("temp", None, Rc::new(|_| Ok(Value::Null)));
        assert!(invoke("temp", &[]).is_some());
        assert!(unregister("temp"));
        assert!(invoke("temp", &[]).is_none());
        // Removing again returns false.
        assert!(!unregister("temp"));
    }

    #[test]
    fn unknown_returns_none() {
        clear();
        assert!(invoke("nope_no_way", &[]).is_none());
    }
}
