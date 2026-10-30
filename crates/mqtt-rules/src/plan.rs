use crate::{
    Event, RuleError,
    expression::{Context, Expression},
};
use serde_json::{Map, Value, json};
use sqlparser::{
    ast::{GroupByExpr, SelectItem, SetExpr, Statement, TableFactor, WildcardAdditionalOptions},
    dialect::GenericDialect,
    parser::Parser,
};
use std::{
    collections::BTreeSet,
    io::{self, Write},
};
const MAX_OUTPUT: usize = 1024 * 1024;
/// Precompiled SQL whitelist. Numeric computation uses IEEE754 f64; integral
/// arithmetic/comparisons outside the exact ±(2^53−1) range fail explicitly.
#[derive(Clone, Debug)]
pub struct CompiledRule {
    filter: String,
    projection: Vec<(String, Expression)>,
    selection: Option<Expression>,
    wildcard: bool,
}
impl CompiledRule {
    pub fn compile(sql: &str) -> Result<Self, RuleError> {
        if sql.is_empty() || sql.len() > 8192 {
            return Err(RuleError::LimitExceeded);
        }
        let statements = Parser::new(&GenericDialect)
            .with_recursion_limit(64)
            .try_with_sql(sql)
            .map_err(|_| RuleError::InvalidSql)?
            .parse_statements()
            .map_err(|_| RuleError::InvalidSql)?;
        if statements.len() != 1 {
            return Err(RuleError::UnsupportedSql);
        }
        let Some(Statement::Query(query)) = statements.first() else {
            return Err(RuleError::UnsupportedSql);
        };
        if query.with.is_some()
            || query.order_by.is_some()
            || query.limit.is_some()
            || !query.limit_by.is_empty()
            || query.offset.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
        {
            return Err(RuleError::UnsupportedSql);
        }
        let SetExpr::Select(select) = &*query.body else {
            return Err(RuleError::UnsupportedSql);
        };
        if select.distinct.is_some()
            || select.top.is_some()
            || select.into.is_some()
            || select.from.len() != 1
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !matches!(&select.group_by,GroupByExpr::Expressions(exprs,modifiers) if exprs.is_empty()&&modifiers.is_empty())
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
            || select.connect_by.is_some()
        {
            return Err(RuleError::UnsupportedSql);
        }
        let source = &select.from[0];
        if !source.joins.is_empty() {
            return Err(RuleError::UnsupportedSql);
        }
        let TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
        } = &source.relation
        else {
            return Err(RuleError::UnsupportedSql);
        };
        if name.0.len() != 1
            || name.0[0].quote_style != Some('"')
            || alias.is_some()
            || args.is_some()
            || !with_hints.is_empty()
            || version.is_some()
            || *with_ordinality
            || !partitions.is_empty()
            || json_path.is_some()
        {
            return Err(RuleError::UnsupportedSql);
        }
        let filter = name.0[0].value.clone();
        if !mqtt_router::validate_filter(&filter) {
            return Err(RuleError::InvalidTopicFilter);
        }
        if select.projection.is_empty() || select.projection.len() > 64 {
            return Err(RuleError::LimitExceeded);
        }
        let mut count = 0;
        let mut projection = Vec::new();
        let mut names = BTreeSet::new();
        let mut wildcard = false;
        for (index, item) in select.projection.iter().enumerate() {
            let (name, expr) = match item {
                SelectItem::Wildcard(options)
                    if select.projection.len() == 1
                        && *options == WildcardAdditionalOptions::default() =>
                {
                    wildcard = true;
                    continue;
                }
                SelectItem::ExprWithAlias { expr, alias } => (alias.value.clone(), expr),
                SelectItem::UnnamedExpr(expr) => {
                    let name = match expr {
                        sqlparser::ast::Expr::Identifier(id) => id.value.clone(),
                        sqlparser::ast::Expr::CompoundIdentifier(ids) => {
                            ids.last().map(|id| id.value.clone()).unwrap_or_default()
                        }
                        _ => format!("expr_{}", index + 1),
                    };
                    (name, expr)
                }
                _ => return Err(RuleError::UnsupportedSql),
            };
            if name.is_empty() || name.len() > 128 || !names.insert(name.clone()) {
                return Err(RuleError::InvalidConfiguration);
            }
            projection.push((name, Expression::compile(expr, 0, &mut count)?));
        }
        let selection = select
            .selection
            .as_ref()
            .map(|e| Expression::compile(e, 0, &mut count))
            .transpose()?;
        Ok(Self {
            filter,
            projection,
            selection,
            wildcard,
        })
    }
    pub fn matches_topic(&self, topic: &str) -> bool {
        mqtt_router::matches_topic_filter(&self.filter, topic)
    }
    pub fn topic_filter(&self) -> &str {
        &self.filter
    }
    pub fn evaluate(&self, event: &Event) -> Result<Option<Value>, RuleError> {
        if event.payload.len() > MAX_OUTPUT
            || event.topic.len() > 65535
            || event.client_id.len() > 65535
            || event.qos > 2
        {
            return Err(RuleError::LimitExceeded);
        }
        if !self.matches_topic(&event.topic) {
            return Ok(None);
        }
        let context = Context::new(event);
        if let Some(selection) = &self.selection
            && selection.evaluate(&context)?.as_bool() != Some(true)
        {
            return Ok(None);
        }
        if self.wildcard {
            let output = json!({"topic":event.topic,"client_id":event.client_id,"qos":event.qos,"retain":event.retain,"timestamp":event.timestamp_ms,"payload":context.payload()});
            check_size(&output)?;
            return Ok(Some(output));
        }
        let mut result = Map::new();
        let mut size = 2usize;
        for (name, expr) in &self.projection {
            let value = expr.evaluate(&context)?;
            size = size
                .checked_add(check_size(&value)?)
                .and_then(|n| n.checked_add(name.len() * 6 + 4))
                .ok_or(RuleError::OutputTooLarge)?;
            if size > MAX_OUTPUT {
                return Err(RuleError::OutputTooLarge);
            }
            result.insert(name.clone(), value);
        }
        Ok(Some(Value::Object(result)))
    }
}
struct Size {
    bytes: usize,
}
impl Write for Size {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        if self.bytes > MAX_OUTPUT {
            return Err(io::Error::other("output limit"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn check_size(value: &Value) -> Result<usize, RuleError> {
    let mut size = Size { bytes: 0 };
    serde_json::to_writer(&mut size, value).map_err(|_| RuleError::OutputTooLarge)?;
    Ok(size.bytes)
}
