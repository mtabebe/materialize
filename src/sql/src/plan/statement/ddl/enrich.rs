// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Expansion of `ENRICH WITH` into the objects that implement it.
//!
//! `CREATE TABLE tickets (body text) ENRICH WITH (severity = ai_classify(body, ...))`
//! declares a column that nothing inside Materialize computes. The engine holds no
//! credential and makes no network call; an external worker does, and these objects
//! are the interface it works against.
//!
//! The statement expands into six items:
//!
//! | # | Name | Kind | Purpose |
//! |---|---|---|---|
//! | 1 | `tickets_raw` | the original relation | rows land here |
//! | 2 | `tickets_ai_store_raw` | table | append-only; the worker writes here |
//! | 3 | `tickets_ai_store` | view | `DISTINCT ON` dedup over #2 |
//! | 4 | `tickets_pending` | view | the work queue: an anti-join of #1 against #3 |
//! | 5 | `tickets_ai_spec` | view | what to compute, for the worker to read |
//! | 6 | `tickets` | view | shadows the declared name; what the user queries |
//!
//! Two of these are load-bearing in ways that are easy to miss.
//!
//! **#3 is not optional.** Tables have no `ON CONFLICT` and webhook sources are
//! append-only, so a retried write leaves two rows. #4 anti-joins against #3 rather
//! than #2 for exactly this reason: against the raw table a duplicate write would
//! leave the input looking unanswered forever.
//!
//! **#5 is what lets a worker be written without reading this file.** It publishes
//! the kind, the prompt or label set and the version for each enriched column, so
//! one worker serves any `ENRICH WITH` clause without reconfiguration. Without it
//! the meaning of `severity` would live in a worker config that no reader of the
//! SQL would ever find.
//!
//! The user's name lands on #6, a view, so rows are inserted into `tickets_raw`
//! rather than `tickets`. That is the one wart the shadowing buys.

use std::collections::BTreeSet;
use std::fmt::Write;

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    EnrichWithItem, Expr, Function, FunctionArgs, Ident, UnresolvedItemName, Value,
};

use crate::names::Aug;
use crate::plan::PlanError;

/// What a single `ENRICH WITH` binding asks for, after validation.
struct Enrichment {
    /// The enriched column this binding produces.
    column: Ident,
    /// `classify`, `extract`, `score`, `generate` or `embed`: the `ai_` prefix
    /// stripped off the declared function.
    kind: &'static str,
    /// The prompt, for the kinds that take one.
    prompt: Option<String>,
    /// The label set, for `classify`.
    labels: Option<Vec<String>>,
    /// The SQL type the enriched column is projected as.
    sql_type: &'static str,
}

/// The layer-1 function family, and what each one declares.
///
/// `arity` counts the input column, so `ai_embed(body)` is 1 and the rest are 2.
const KINDS: &[(&str, &str, usize, &str)] = &[
    ("ai_classify", "classify", 2, "text"),
    ("ai_extract", "extract", 2, "text"),
    ("ai_score", "score", 2, "double precision"),
    ("ai_generate", "generate", 2, "text"),
    ("ai_embed", "embed", 1, "float4[]"),
];

/// Appends a suffix to the last identifier of a name, leaving qualification alone.
///
/// The generated objects live in whatever schema the declared name resolved to, so
/// `myschema.tickets` yields `myschema.tickets_pending`.
pub fn derive_name(
    name: &UnresolvedItemName,
    suffix: &str,
) -> Result<UnresolvedItemName, PlanError> {
    let mut parts = name.0.clone();
    let last = parts
        .last_mut()
        .expect("an item name always has at least one part");
    let derived = Ident::new(format!("{}{}", last.as_str(), suffix)).map_err(|e| {
        PlanError::Unstructured(format!(
            "ENRICH WITH cannot derive a name for {name}: {}",
            e.to_string()
        ))
    })?;
    *last = derived;
    Ok(UnresolvedItemName(parts))
}

/// The statements a single `ENRICH WITH` clause expands into, in dependency order.
#[derive(Debug)]
pub struct Expansion {
    /// The name the underlying relation must be created under.
    pub raw_name: UnresolvedItemName,
    /// Items 2 through 6, as SQL.
    ///
    /// SQL rather than plans because they cannot be planned yet: each one references
    /// an item that does not exist until the previous statement has committed. It is
    /// also what the catalog stores and re-plans at boot, so the text is exercised
    /// twice rather than once.
    pub statements: Vec<String>,
}

/// Validates an `ENRICH WITH` clause and renders the objects that implement it.
pub fn expand(
    name: &UnresolvedItemName,
    items: &[EnrichWithItem<Aug>],
) -> Result<Expansion, PlanError> {
    if items.is_empty() {
        sql_bail!("ENRICH WITH requires at least one enrichment");
    }

    let mut input_column: Option<Ident> = None;
    let mut seen = BTreeSet::new();
    let mut enrichments = Vec::with_capacity(items.len());
    for item in items {
        if !seen.insert(item.name.clone()) {
            // The enriched view projects one column per binding, so a repeat would
            // silently produce two columns of the same name.
            sql_bail!("ENRICH WITH declares {} more than once", item.name);
        }
        let (input, enrichment) = plan_item(item)?;
        match &input_column {
            // One store per relation, keyed on one input, so one worker call fills
            // every enriched column for a row at once. Two input columns would mean
            // two stores and two pending views, which is a shape the demo does not
            // build.
            Some(first) if first != &input => sql_bail!(
                "ENRICH WITH enrichments must all read the same column, but {} reads {} \
                 and {} reads {}",
                items[0].name,
                first,
                item.name,
                input
            ),
            Some(_) => {}
            None => input_column = Some(input),
        }
        enrichments.push(enrichment);
    }
    let input_column = input_column.expect("items is non-empty");

    let raw_name = derive_name(name, "_raw")?;
    let store_raw = derive_name(name, "_ai_store_raw")?;
    let store = derive_name(name, "_ai_store")?;
    let pending = derive_name(name, "_pending")?;
    let spec = derive_name(name, "_ai_spec")?;

    // Derived from the clause rather than typed by hand, so editing a prompt changes
    // the version and re-queues every row without a manual bump.
    let version = prompt_version(items);

    let statements = vec![
        render_store_raw(&store_raw),
        render_store(&store, &store_raw),
        render_pending(&pending, &raw_name, &store, &input_column, &version),
        render_spec(&spec, &enrichments, &input_column, &version),
        render_enriched(
            name,
            &raw_name,
            &store,
            &input_column,
            &version,
            &enrichments,
        ),
    ];

    Ok(Expansion {
        raw_name,
        statements,
    })
}

/// Reads one `name = ai_*(column, ...)` binding.
fn plan_item(item: &EnrichWithItem<Aug>) -> Result<(Ident, Enrichment), PlanError> {
    let Expr::Function(Function {
        name,
        args: FunctionArgs::Args { args, order_by },
        filter: None,
        over: None,
        distinct: false,
    }) = &item.expr
    else {
        sql_bail!(
            "ENRICH WITH expects {} to be a call to an ai_* function",
            item.name
        );
    };
    if !order_by.is_empty() {
        sql_bail!("ENRICH WITH does not accept ORDER BY");
    }

    let func = name.full_item_name().item.as_str();
    let Some((_, kind, arity, sql_type)) = KINDS.iter().find(|(f, ..)| *f == func) else {
        sql_bail!(
            "{func} is not an enrichment; ENRICH WITH accepts {}",
            KINDS
                .iter()
                .map(|(f, ..)| *f)
                .collect::<Vec<_>>()
                .join(", ")
        );
    };
    if args.len() != *arity {
        sql_bail!("{func} takes {arity} argument(s), got {}", args.len());
    }

    // The first argument names the column the model reads. It must be a bare column
    // reference: the pending view joins the store against it, so it has to be
    // something the worker can echo back as a key.
    let Expr::Identifier(parts) = &args[0] else {
        sql_bail!("the first argument to {func} must be a column of the relation");
    };
    let [input] = &parts[..] else {
        sql_bail!("the first argument to {func} must be an unqualified column name");
    };

    let (prompt, labels) = match (*kind, args.get(1)) {
        ("classify", Some(Expr::Array(elements))) => {
            let mut labels = Vec::with_capacity(elements.len());
            for element in elements {
                match element {
                    Expr::Value(Value::String(s)) => labels.push(s.clone()),
                    _ => sql_bail!("ai_classify labels must be string literals"),
                }
            }
            if labels.is_empty() {
                sql_bail!("ai_classify requires at least one label");
            }
            (None, Some(labels))
        }
        ("classify", _) => sql_bail!("ai_classify requires an array of string labels"),
        ("embed", _) => (None, None),
        (_, Some(Expr::Value(Value::String(s)))) => (Some(s.clone()), None),
        _ => sql_bail!("the second argument to {func} must be a string literal prompt"),
    };

    Ok((
        input.clone(),
        Enrichment {
            column: item.name.clone(),
            kind,
            prompt,
            labels,
            sql_type,
        },
    ))
}

/// A stable digest of the clause, used to stamp results and to invalidate them.
///
/// FNV-1a over the clause's canonical rendering. It must be stable across restarts
/// and across processes, since the value is persisted in every stored row and
/// compared against on every read, which rules out anything seeded per-process.
fn prompt_version(items: &[EnrichWithItem<Aug>]) -> String {
    let canonical = items
        .iter()
        .map(|i| i.to_ast_string_simple())
        .collect::<Vec<_>>()
        .join(", ");
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in canonical.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("v_{:06x}", hash & 0xffffff)
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn render_store_raw(store_raw: &UnresolvedItemName) -> String {
    format!(
        "CREATE TABLE {store_raw} (\
         input text, prompt_version text, output jsonb, computed_at timestamptz)"
    )
}

fn render_store(store: &UnresolvedItemName, store_raw: &UnresolvedItemName) -> String {
    // The dedup rule lives here and only here, so both consumers inherit it.
    format!(
        "CREATE VIEW {store} AS \
         SELECT DISTINCT ON (input, prompt_version) input, prompt_version, output, computed_at \
         FROM {store_raw} \
         ORDER BY input, prompt_version, computed_at DESC"
    )
}

fn render_pending(
    pending: &UnresolvedItemName,
    raw_name: &UnresolvedItemName,
    store: &UnresolvedItemName,
    input: &Ident,
    version: &str,
) -> String {
    // `DISTINCT` is what makes five hundred tickets with one body cost one call: the
    // work queue is keyed on the input, not on the row.
    let version = quote_literal(version);
    format!(
        "CREATE VIEW {pending} AS \
         SELECT DISTINCT r.{input} AS input \
         FROM {raw_name} AS r \
         LEFT JOIN {store} AS s ON r.{input} = s.input AND s.prompt_version = {version} \
         WHERE s.input IS NULL"
    )
}

fn render_spec(
    spec: &UnresolvedItemName,
    enrichments: &[Enrichment],
    input: &Ident,
    version: &str,
) -> String {
    let mut sql = format!(
        "CREATE VIEW {spec} \
         (column_name, kind, input_column, prompt, labels, prompt_version) AS VALUES "
    );
    for (i, e) in enrichments.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let prompt = match &e.prompt {
            // The casts are load-bearing: a column that is NULL in every row would
            // otherwise have no type for the worker to read it back through.
            Some(p) => format!("{}::text", quote_literal(p)),
            None => "NULL::text".to_string(),
        };
        let labels = match &e.labels {
            Some(labels) => format!(
                "ARRAY[{}]::text[]",
                labels
                    .iter()
                    .map(|l| quote_literal(l))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            None => "NULL::text[]".to_string(),
        };
        write!(
            sql,
            "({}::text, {}::text, {}::text, {prompt}, {labels}, {}::text)",
            quote_literal(e.column.as_str()),
            quote_literal(e.kind),
            quote_literal(input.as_str()),
            quote_literal(version),
        )
        .expect("writing to a String cannot fail");
    }
    sql
}

fn render_enriched(
    name: &UnresolvedItemName,
    raw_name: &UnresolvedItemName,
    store: &UnresolvedItemName,
    input: &Ident,
    version: &str,
    enrichments: &[Enrichment],
) -> String {
    let mut projection = String::from("r.*");
    for e in enrichments {
        // `LEFT JOIN`, so a row is visible with NULL labels from the moment it lands
        // and fills in when its answer arrives, rather than being invisible until
        // then.
        write!(
            projection,
            ", (s.output ->> {})::{} AS {}",
            quote_literal(e.column.as_str()),
            e.sql_type,
            e.column
        )
        .expect("writing to a String cannot fail");
    }
    let version = quote_literal(version);
    format!(
        "CREATE VIEW {name} AS SELECT {projection} \
         FROM {raw_name} AS r \
         LEFT JOIN {store} AS s ON r.{input} = s.input AND s.prompt_version = {version}"
    )
}
