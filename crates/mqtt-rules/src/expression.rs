//! Compiled whitelist expressions, evaluated without executing SQL or IO.
use crate::{Event, RuleError};
use serde_json::{Number, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator,
    Value as SqlValue,
};
use std::{cell::OnceCell, cmp::Ordering};

#[derive(Clone, Debug)]
pub(crate) enum Expression {
    Literal(Value),
    Field(Vec<String>),
    Unary(UnaryOperator, Box<Self>),
    Binary(Box<Self>, BinaryOperator, Box<Self>),
    IsNull(Box<Self>, bool),
    Function(String, Vec<Self>),
}
pub(crate) struct Context<'a> {
    pub event: &'a Event,
    payload: OnceCell<Option<Value>>,
}
impl<'a> Context<'a> {
    pub fn new(event: &'a Event) -> Self {
        Self {
            event,
            payload: OnceCell::new(),
        }
    }
    pub fn payload(&self) -> Option<&Value> {
        self.payload
            .get_or_init(|| serde_json::from_slice(&self.event.payload).ok())
            .as_ref()
    }
}
impl Expression {
    pub fn compile(expr: &Expr, depth: usize, count: &mut usize) -> Result<Self, RuleError> {
        *count += 1;
        if depth > 32 || *count > 256 {
            return Err(RuleError::LimitExceeded);
        }
        let child = |e: &Expr, count: &mut usize| Self::compile(e, depth + 1, count);
        Ok(match expr {
            Expr::Value(v) => Self::Literal(match v {
                SqlValue::Null => Value::Null,
                SqlValue::Boolean(b) => Value::Bool(*b),
                SqlValue::SingleQuotedString(s) => Value::String(s.clone()),
                SqlValue::Number(s, _) => {
                    let n = s.parse::<f64>().map_err(|_| RuleError::InvalidSql)?;
                    if !n.is_finite() || n.abs() > 9_007_199_254_740_991.0 {
                        return Err(RuleError::InvalidSql);
                    }
                    Value::Number(Number::from_f64(n).ok_or(RuleError::InvalidSql)?)
                }
                _ => return Err(RuleError::UnsupportedSql),
            }),
            Expr::Identifier(id) => Self::field(vec![id.value.clone()])?,
            Expr::CompoundIdentifier(ids) => {
                Self::field(ids.iter().map(|id| id.value.clone()).collect())?
            }
            Expr::Nested(e) => child(e, count)?,
            Expr::UnaryOp { op, expr }
                if matches!(
                    op,
                    UnaryOperator::Not | UnaryOperator::Minus | UnaryOperator::Plus
                ) =>
            {
                Self::Unary(*op, Box::new(child(expr, count)?))
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::And
                        | BinaryOperator::Or
                        | BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::Plus
                        | BinaryOperator::Minus
                        | BinaryOperator::Multiply
                        | BinaryOperator::Divide
                ) =>
            {
                Self::Binary(
                    Box::new(child(left, count)?),
                    op.clone(),
                    Box::new(child(right, count)?),
                )
            }
            Expr::IsNull(e) => Self::IsNull(Box::new(child(e, count)?), false),
            Expr::IsNotNull(e) => Self::IsNull(Box::new(child(e, count)?), true),
            Expr::Function(f) => {
                if f.name.0.len() != 1
                    || f.uses_odbc_syntax
                    || !matches!(f.parameters, FunctionArguments::None)
                    || f.filter.is_some()
                    || f.null_treatment.is_some()
                    || f.over.is_some()
                    || !f.within_group.is_empty()
                {
                    return Err(RuleError::UnsupportedSql);
                }
                let name = f.name.0[0].value.to_ascii_uppercase();
                let FunctionArguments::List(args) = &f.args else {
                    return Err(RuleError::UnsupportedSql);
                };
                if args.duplicate_treatment.is_some() || !args.clauses.is_empty() {
                    return Err(RuleError::UnsupportedSql);
                }
                if !matches!(name.as_str(), "COALESCE" | "LOWER" | "UPPER")
                    || args.args.is_empty()
                    || (name != "COALESCE" && args.args.len() != 1)
                {
                    return Err(RuleError::UnsupportedSql);
                }
                let mut compiled = Vec::new();
                for arg in &args.args {
                    let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg else {
                        return Err(RuleError::UnsupportedSql);
                    };
                    compiled.push(child(e, count)?);
                }
                Self::Function(name, compiled)
            }
            _ => return Err(RuleError::UnsupportedSql),
        })
    }
    fn field(path: Vec<String>) -> Result<Self, RuleError> {
        let valid = path.first().is_some_and(|name| {
            if name == "payload" {
                true
            } else {
                path.len() == 1
                    && matches!(
                        name.as_str(),
                        "topic" | "client_id" | "qos" | "retain" | "timestamp"
                    )
            }
        });
        if !valid {
            return Err(RuleError::UnsupportedSql);
        }
        Ok(Self::Field(path))
    }
    pub fn evaluate(&self, c: &Context<'_>) -> Result<Value, RuleError> {
        Ok(match self {
            Self::Literal(v) => v.clone(),
            Self::Field(path) => match path[0].as_str() {
                "topic" => Value::String(c.event.topic.clone()),
                "client_id" => Value::String(c.event.client_id.clone()),
                "qos" => Value::from(c.event.qos),
                "retain" => Value::Bool(c.event.retain),
                "timestamp" => Value::from(c.event.timestamp_ms),
                _ => {
                    let mut v = c.payload();
                    for key in &path[1..] {
                        v = v
                            .and_then(|value| value.as_object())
                            .and_then(|object| object.get(key));
                    }
                    v.cloned().unwrap_or(Value::Null)
                }
            },
            Self::Unary(op, e) => {
                let value = e.evaluate(c)?;
                match op {
                    UnaryOperator::Not => truth(value.as_bool().map(|b| !b)),
                    UnaryOperator::Plus => numeric(&value)?
                        .map(number)
                        .transpose()?
                        .unwrap_or(Value::Null),
                    _ => numeric(&value)?
                        .map(|n| number(-n))
                        .transpose()?
                        .unwrap_or(Value::Null),
                }
            }
            Self::IsNull(e, negate) => Value::Bool(e.evaluate(c)?.is_null() != *negate),
            Self::Binary(left, op, right) => {
                let l = left.evaluate(c)?;
                if *op == BinaryOperator::And && l == Value::Bool(false) {
                    return Ok(Value::Bool(false));
                }
                if *op == BinaryOperator::Or && l == Value::Bool(true) {
                    return Ok(Value::Bool(true));
                }
                let r = right.evaluate(c)?;
                match op {
                    BinaryOperator::And => truth(match (l.as_bool(), r.as_bool()) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    }),
                    BinaryOperator::Or => truth(match (l.as_bool(), r.as_bool()) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    }),
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => match (numeric(&l)?, numeric(&r)?) {
                        (Some(a), Some(b)) => {
                            if *op == BinaryOperator::Divide && b == 0.0 {
                                return Err(RuleError::Arithmetic);
                            }
                            number(match op {
                                BinaryOperator::Plus => a + b,
                                BinaryOperator::Minus => a - b,
                                BinaryOperator::Multiply => a * b,
                                _ => a / b,
                            })?
                        }
                        _ => Value::Null,
                    },
                    _ => {
                        let order = compare(&l, &r)?;
                        truth(order.map(|order| match op {
                            BinaryOperator::Eq => order == Ordering::Equal,
                            BinaryOperator::NotEq => order != Ordering::Equal,
                            BinaryOperator::Lt => order == Ordering::Less,
                            BinaryOperator::LtEq => order != Ordering::Greater,
                            BinaryOperator::Gt => order == Ordering::Greater,
                            _ => order != Ordering::Less,
                        }))
                    }
                }
            }
            Self::Function(name, args) => {
                if name == "COALESCE" {
                    let mut selected = Value::Null;
                    for arg in args {
                        selected = arg.evaluate(c)?;
                        if !selected.is_null() {
                            break;
                        }
                    }
                    selected
                } else {
                    match args[0].evaluate(c)? {
                        Value::String(s) => Value::String(if name == "LOWER" {
                            s.to_lowercase()
                        } else {
                            s.to_uppercase()
                        }),
                        _ => Value::Null,
                    }
                }
            }
        })
    }
}
fn truth(value: Option<bool>) -> Value {
    value.map(Value::Bool).unwrap_or(Value::Null)
}
fn number(n: f64) -> Result<Value, RuleError> {
    Number::from_f64(n)
        .map(Value::Number)
        .ok_or(RuleError::Arithmetic)
}
fn numeric(v: &Value) -> Result<Option<f64>, RuleError> {
    let Value::Number(n) = v else { return Ok(None) };
    if n.as_i64()
        .is_some_and(|n| n.unsigned_abs() > 9_007_199_254_740_991)
        || n.as_u64().is_some_and(|n| n > 9_007_199_254_740_991)
    {
        return Err(RuleError::Arithmetic);
    }
    Ok(n.as_f64())
}
fn compare(a: &Value, b: &Value) -> Result<Option<Ordering>, RuleError> {
    Ok(match (a, b) {
        (Value::Number(_), Value::Number(_)) => match (numeric(a)?, numeric(b)?) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            _ => None,
        },
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    })
}
