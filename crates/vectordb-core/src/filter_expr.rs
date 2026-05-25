//! String filter expression parser.
//!
//! Lets users write filters as plain-text expressions (familiar to anyone who
//! has used Milvus / SQL `WHERE`) instead of the structured JSON DSL. The
//! parser compiles them down to the existing [`Filter`] / [`Condition`]
//! types in [`crate::filter`] — there is **no** new evaluator; this is
//! purely a front-end.
//!
//! ## Grammar
//!
//! ```text
//! expr     := or
//! or       := and ( ('||' | 'or')  and )*
//! and      := unary ( ('&&' | 'and') unary )*
//! unary    := ('!' | 'not') unary
//!           | atom
//! atom     := '(' expr ')'
//!           | 'exists' '(' KEY ')'
//!           | KEY 'in'      list
//!           | KEY 'not' 'in' list
//!           | KEY OP LITERAL
//! list     := '[' LITERAL (',' LITERAL)* ']'
//! OP       := '==' | '!=' | '>' | '>=' | '<' | '<='
//! LITERAL  := NUMBER | STRING | 'true' | 'false' | 'null'
//! KEY      := IDENT ( '.' IDENT )*
//! ```
//!
//! Strings can be single- or double-quoted. `NULL` / `TRUE` / `FALSE` are
//! also accepted (case-insensitive).

use serde_json::Value;
use thiserror::Error;

use crate::filter::{Condition, FieldCondition, FieldOp, Filter};

/// Parse gateway / gRPC filter input: JSON object **or** string expression.
///
/// Empty input → `Ok(None)` (no filter). JSON must start with `{`.
pub fn parse_filter_input(input: &str) -> Result<Option<Filter>, FilterParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.starts_with('{') {
        let f: Filter = serde_json::from_str(trimmed).map_err(|e| FilterParseError {
            message: format!("invalid filter JSON: {e}"),
            near: trimmed.chars().take(32).collect(),
            pos: 0,
        })?;
        return Ok(Some(f));
    }
    parse_filter_expr(trimmed).map(Some)
}

/// Parse a string expression into the structured [`Filter`] used by the engine.
///
/// Empty / whitespace-only input yields a default (empty) filter that matches
/// every payload.
pub fn parse_filter_expr(input: &str) -> Result<Filter, FilterParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Filter::default());
    }
    let tokens = lex(trimmed)?;
    let mut p = Parser { tokens, pos: 0 };
    let node = p.parse_expr()?;
    if p.pos != p.tokens.len() {
        return Err(FilterParseError::new(
            "unexpected trailing tokens",
            p.current_span(),
        ));
    }
    Ok(lower(node))
}

#[derive(Debug, Error, Clone, PartialEq)]
#[error("filter expression: {message} (near `{near}` at position {pos})")]
pub struct FilterParseError {
    pub message: String,
    pub near: String,
    pub pos: usize,
}

impl FilterParseError {
    fn new(msg: impl Into<String>, span: (usize, String)) -> Self {
        Self {
            message: msg.into(),
            near: span.1,
            pos: span.0,
        }
    }
}

// ---------------------------------------------------------------- AST --

/// Intermediate boolean tree. `lower()` collapses it into the engine's
/// `must` / `must_not` / `should` shape.
#[derive(Debug, Clone, PartialEq)]
enum Node {
    And(Vec<Node>),
    Or(Vec<Node>),
    Not(Box<Node>),
    Field(FieldCondition),
}

fn lower(node: Node) -> Filter {
    match node {
        Node::And(children) => {
            let mut out = Filter::default();
            for child in children {
                let f = lower(child);
                // Inline nested `must` so common cases stay flat.
                if f.must_not.is_empty() && f.should.is_empty() {
                    out.must.extend(f.must);
                } else {
                    out.must.push(Condition::Nested { filter: Box::new(f) });
                }
            }
            out
        }
        Node::Or(children) => {
            let mut out = Filter::default();
            for child in children {
                let f = lower(child);
                if f.must_not.is_empty() && f.should.is_empty() && f.must.len() == 1 {
                    out.should.push(f.must.into_iter().next().unwrap());
                } else {
                    out.should.push(Condition::Nested { filter: Box::new(f) });
                }
            }
            out
        }
        Node::Not(inner) => {
            let f = lower(*inner);
            if f.must_not.is_empty() && f.should.is_empty() && f.must.len() == 1 {
                Filter {
                    must_not: vec![f.must.into_iter().next().unwrap()],
                    ..Filter::default()
                }
            } else {
                Filter {
                    must_not: vec![Condition::Nested { filter: Box::new(f) }],
                    ..Filter::default()
                }
            }
        }
        Node::Field(fc) => Filter {
            must: vec![Condition::Field(fc)],
            ..Filter::default()
        },
    }
}

// ------------------------------------------------------------- Tokens --

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),     // user identifier (or keyword in lowercase)
    Number(f64),
    String(String),
    Bool(bool),
    Null,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    EqEq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
    AndAnd,
    OrOr,
    Bang,
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    pos: usize,
    text: String,
}

fn lex(s: &str) -> Result<Vec<Token>, FilterParseError> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        // multi-char operators
        if i + 1 < bytes.len() {
            let two = &s[i..i + 2];
            let t = match two {
                "==" => Some(Tok::EqEq),
                "!=" => Some(Tok::NotEq),
                ">=" => Some(Tok::Ge),
                "<=" => Some(Tok::Le),
                "&&" => Some(Tok::AndAnd),
                "||" => Some(Tok::OrOr),
                _ => None,
            };
            if let Some(tok) = t {
                out.push(Token {
                    tok,
                    pos: start,
                    text: two.to_string(),
                });
                i += 2;
                continue;
            }
        }
        let single = match c {
            '(' => Some(Tok::LParen),
            ')' => Some(Tok::RParen),
            '[' => Some(Tok::LBracket),
            ']' => Some(Tok::RBracket),
            ',' => Some(Tok::Comma),
            '.' => Some(Tok::Dot),
            '<' => Some(Tok::Lt),
            '>' => Some(Tok::Gt),
            '!' => Some(Tok::Bang),
            _ => None,
        };
        if let Some(tok) = single {
            out.push(Token {
                tok,
                pos: start,
                text: c.to_string(),
            });
            i += 1;
            continue;
        }
        if c == '\'' || c == '"' {
            let quote = c;
            i += 1;
            let mut buf = String::new();
            while i < bytes.len() {
                let ch = bytes[i] as char;
                if ch == '\\' && i + 1 < bytes.len() {
                    let next = bytes[i + 1] as char;
                    buf.push(match next {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '\\' => '\\',
                        '\'' => '\'',
                        '"' => '"',
                        other => other,
                    });
                    i += 2;
                    continue;
                }
                if ch == quote {
                    break;
                }
                buf.push(ch);
                i += 1;
            }
            if i >= bytes.len() {
                return Err(FilterParseError::new(
                    "unterminated string literal",
                    (start, s[start..].chars().take(8).collect()),
                ));
            }
            i += 1; // consume closing quote
            out.push(Token {
                tok: Tok::String(buf),
                pos: start,
                text: s[start..i].to_string(),
            });
            continue;
        }
        if c.is_ascii_digit() || (c == '-' && i + 1 < bytes.len() && (bytes[i + 1] as char).is_ascii_digit()) {
            let mut j = i + 1;
            let mut seen_dot = false;
            while j < bytes.len() {
                let ch = bytes[j] as char;
                if ch.is_ascii_digit() {
                    j += 1;
                } else if ch == '.' && !seen_dot {
                    seen_dot = true;
                    j += 1;
                } else if (ch == 'e' || ch == 'E') && j + 1 < bytes.len() {
                    seen_dot = true;
                    j += 1;
                    if bytes[j] as char == '+' || bytes[j] as char == '-' {
                        j += 1;
                    }
                } else {
                    break;
                }
            }
            let num: f64 = s[i..j]
                .parse()
                .map_err(|_| FilterParseError::new("invalid number", (start, s[i..j].to_string())))?;
            out.push(Token {
                tok: Tok::Number(num),
                pos: start,
                text: s[i..j].to_string(),
            });
            i = j;
            continue;
        }
        if c.is_alphabetic() || c == '_' {
            let mut j = i + 1;
            while j < bytes.len()
                && (bytes[j] as char).is_ascii_alphanumeric()
                || (j < bytes.len() && bytes[j] as char == '_')
            {
                j += 1;
            }
            let word = &s[i..j];
            let tok = match word.to_ascii_lowercase().as_str() {
                "true" => Tok::Bool(true),
                "false" => Tok::Bool(false),
                "null" => Tok::Null,
                _ => Tok::Ident(word.to_string()),
            };
            out.push(Token {
                tok,
                pos: start,
                text: word.to_string(),
            });
            i = j;
            continue;
        }
        return Err(FilterParseError::new(
            format!("unexpected character `{c}`"),
            (start, c.to_string()),
        ));
    }
    Ok(out)
}

// ------------------------------------------------------------- Parser --

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos).map(|t| &t.tok)
    }
    fn bump(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }
    fn expect(&mut self, want: &Tok, label: &str) -> Result<Token, FilterParseError> {
        match self.peek() {
            Some(t) if same_tag(t, want) => Ok(self.bump().unwrap()),
            _ => Err(FilterParseError::new(
                format!("expected {label}"),
                self.current_span(),
            )),
        }
    }
    fn current_span(&self) -> (usize, String) {
        if let Some(t) = self.tokens.get(self.pos) {
            (t.pos, t.text.clone())
        } else if let Some(t) = self.tokens.last() {
            (t.pos + t.text.len(), String::from("end of input"))
        } else {
            (0, String::from("empty"))
        }
    }

    fn parse_expr(&mut self) -> Result<Node, FilterParseError> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(Tok::OrOr))
            || self.peek_kw_eq("or")
        {
            self.bump();
            let right = self.parse_and()?;
            left = match left {
                Node::Or(mut v) => {
                    v.push(right);
                    Node::Or(v)
                }
                _ => Node::Or(vec![left, right]),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Node, FilterParseError> {
        let mut left = self.parse_unary()?;
        while matches!(self.peek(), Some(Tok::AndAnd))
            || self.peek_kw_eq("and")
        {
            self.bump();
            let right = self.parse_unary()?;
            left = match left {
                Node::And(mut v) => {
                    v.push(right);
                    Node::And(v)
                }
                _ => Node::And(vec![left, right]),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Node, FilterParseError> {
        if matches!(self.peek(), Some(Tok::Bang)) || self.peek_kw_eq("not") {
            self.bump();
            let inner = self.parse_unary()?;
            return Ok(Node::Not(Box::new(inner)));
        }
        self.parse_atom()
    }

    fn parse_atom(&mut self) -> Result<Node, FilterParseError> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.bump();
            let inner = self.parse_expr()?;
            self.expect(&Tok::RParen, "`)`")?;
            return Ok(inner);
        }
        // exists(KEY)
        if self.peek_kw_eq("exists") {
            self.bump();
            self.expect(&Tok::LParen, "`(` after exists")?;
            let key = self.parse_key()?;
            self.expect(&Tok::RParen, "`)` after exists key")?;
            return Ok(Node::Field(FieldCondition {
                key,
                op: FieldOp::Exists { exists: true },
            }));
        }
        // key ...
        let key = self.parse_key()?;
        // optional `not in` for negated membership
        if self.peek_kw_eq("not") {
            self.bump();
            self.expect_kw("in", "`in` after `not`")?;
            let values = self.parse_list()?;
            return Ok(Node::Not(Box::new(Node::Field(FieldCondition {
                key,
                op: FieldOp::AnyOf { any: values },
            }))));
        }
        if self.peek_kw_eq("in") {
            self.bump();
            let values = self.parse_list()?;
            return Ok(Node::Field(FieldCondition {
                key,
                op: FieldOp::AnyOf { any: values },
            }));
        }
        // operator
        let op_tok = self.bump().ok_or_else(|| {
            FilterParseError::new("expected operator after field name", self.current_span())
        })?;
        let lit = self.parse_literal()?;
        let op = match op_tok.tok {
            Tok::EqEq => FieldOp::Match { value: lit },
            Tok::NotEq => {
                return Ok(Node::Not(Box::new(Node::Field(FieldCondition {
                    key,
                    op: FieldOp::Match { value: lit },
                }))));
            }
            Tok::Gt => FieldOp::Range {
                gt: Some(value_to_f64(&lit, &op_tok)?),
                gte: None,
                lt: None,
                lte: None,
            },
            Tok::Ge => FieldOp::Range {
                gt: None,
                gte: Some(value_to_f64(&lit, &op_tok)?),
                lt: None,
                lte: None,
            },
            Tok::Lt => FieldOp::Range {
                gt: None,
                gte: None,
                lt: Some(value_to_f64(&lit, &op_tok)?),
                lte: None,
            },
            Tok::Le => FieldOp::Range {
                gt: None,
                gte: None,
                lt: None,
                lte: Some(value_to_f64(&lit, &op_tok)?),
            },
            _ => {
                return Err(FilterParseError::new(
                    "expected operator (==, !=, <, <=, >, >=, in)",
                    (op_tok.pos, op_tok.text),
                ));
            }
        };
        Ok(Node::Field(FieldCondition { key, op }))
    }

    fn parse_key(&mut self) -> Result<String, FilterParseError> {
        let first = match self.bump() {
            Some(t) => match t.tok {
                Tok::Ident(s) => s,
                _ => {
                    return Err(FilterParseError::new(
                        "expected field name",
                        (t.pos, t.text),
                    ))
                }
            },
            None => {
                return Err(FilterParseError::new(
                    "expected field name",
                    self.current_span(),
                ))
            }
        };
        let mut key = first;
        while matches!(self.peek(), Some(Tok::Dot)) {
            self.bump();
            let next = self.bump().ok_or_else(|| {
                FilterParseError::new("expected identifier after `.`", self.current_span())
            })?;
            match next.tok {
                Tok::Ident(s) => {
                    key.push('.');
                    key.push_str(&s);
                }
                _ => {
                    return Err(FilterParseError::new(
                        "expected identifier after `.`",
                        (next.pos, next.text),
                    ))
                }
            }
        }
        Ok(key)
    }

    fn parse_list(&mut self) -> Result<Vec<Value>, FilterParseError> {
        self.expect(&Tok::LBracket, "`[`")?;
        let mut out = Vec::new();
        if matches!(self.peek(), Some(Tok::RBracket)) {
            self.bump();
            return Ok(out);
        }
        loop {
            out.push(self.parse_literal()?);
            match self.peek() {
                Some(Tok::Comma) => {
                    self.bump();
                }
                Some(Tok::RBracket) => {
                    self.bump();
                    break;
                }
                _ => {
                    return Err(FilterParseError::new(
                        "expected `,` or `]` in list",
                        self.current_span(),
                    ))
                }
            }
        }
        Ok(out)
    }

    fn parse_literal(&mut self) -> Result<Value, FilterParseError> {
        let t = self.bump().ok_or_else(|| {
            FilterParseError::new("expected literal", self.current_span())
        })?;
        match t.tok {
            Tok::Number(n) => {
                if n.fract() == 0.0 && n.is_finite() && n.abs() < i64::MAX as f64 {
                    Ok(Value::from(n as i64))
                } else {
                    Ok(serde_json::Number::from_f64(n)
                        .map(Value::Number)
                        .unwrap_or(Value::Null))
                }
            }
            Tok::String(s) => Ok(Value::String(s)),
            Tok::Bool(b) => Ok(Value::Bool(b)),
            Tok::Null => Ok(Value::Null),
            _ => Err(FilterParseError::new(
                "expected literal (number, string, true/false, null)",
                (t.pos, t.text),
            )),
        }
    }

    fn peek_kw_eq(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn expect_kw(&mut self, kw: &str, label: &str) -> Result<(), FilterParseError> {
        if self.peek_kw_eq(kw) {
            self.bump();
            Ok(())
        } else {
            Err(FilterParseError::new(
                format!("expected {label}"),
                self.current_span(),
            ))
        }
    }
}

fn value_to_f64(v: &Value, near: &Token) -> Result<f64, FilterParseError> {
    match v {
        Value::Number(n) => n.as_f64().ok_or_else(|| {
            FilterParseError::new("non-finite number", (near.pos, near.text.clone()))
        }),
        _ => Err(FilterParseError::new(
            "range operators require a numeric literal",
            (near.pos, near.text.clone()),
        )),
    }
}

/// Compare token enum *tags only* (ignoring payload like the inner string of `Ident`).
fn same_tag(a: &Tok, b: &Tok) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

// --------------------------------------------------------------- Tests --

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(s: &str) -> Filter {
        parse_filter_expr(s).unwrap_or_else(|e| panic!("parse `{s}`: {e}"))
    }

    fn payload_books() -> Value {
        json!({
            "category": "books",
            "price": 29.5,
            "tags": ["bestseller", "fiction"],
            "in_stock": true,
            "meta": {"author": "ada"}
        })
    }

    #[test]
    fn empty_input_matches_everything() {
        let f = parse("");
        assert!(f.matches(&payload_books()));
        assert!(f.is_empty());
    }

    #[test]
    fn simple_equality() {
        let f = parse("category == 'books'");
        assert!(f.matches(&payload_books()));
        let f2 = parse("category == 'movies'");
        assert!(!f2.matches(&payload_books()));
    }

    #[test]
    fn not_equal_uses_must_not() {
        let f = parse("category != 'movies'");
        assert!(f.matches(&payload_books()));
        let f2 = parse("category != 'books'");
        assert!(!f2.matches(&payload_books()));
    }

    #[test]
    fn range_operators() {
        let f = parse("price >= 10 && price < 50");
        assert!(f.matches(&payload_books()));
        let f2 = parse("price > 100");
        assert!(!f2.matches(&payload_books()));
    }

    #[test]
    fn in_list_and_dotted_path() {
        let f = parse("meta.author in ['ada', 'bob']");
        assert!(f.matches(&payload_books()));
    }

    #[test]
    fn not_in_list() {
        let f = parse("category not in ['movies', 'music']");
        assert!(f.matches(&payload_books()));
    }

    #[test]
    fn boolean_combinations() {
        let f = parse("(category == 'books' || category == 'movies') && in_stock == true");
        assert!(f.matches(&payload_books()));
    }

    #[test]
    fn boolean_and_with_double_amp_or_keyword_and() {
        let f1 = parse("category == 'books' && price > 10");
        let f2 = parse("category == 'books' and price > 10");
        // Same semantics regardless of operator style
        assert!(f1.matches(&payload_books()));
        assert!(f2.matches(&payload_books()));
    }

    #[test]
    fn negation_with_bang_and_keyword() {
        let f1 = parse("!(category == 'movies')");
        let f2 = parse("not category == 'movies'");
        assert!(f1.matches(&payload_books()));
        assert!(f2.matches(&payload_books()));
    }

    #[test]
    fn exists_function() {
        let f = parse("exists(meta.author)");
        assert!(f.matches(&payload_books()));
        let f2 = parse("exists(meta.missing)");
        assert!(!f2.matches(&payload_books()));
    }

    #[test]
    fn unterminated_string_errors() {
        let err = parse_filter_expr("category == 'books").unwrap_err();
        assert!(err.message.contains("unterminated"));
    }

    #[test]
    fn trailing_garbage_errors() {
        let err = parse_filter_expr("category == 'books' garbage").unwrap_err();
        assert!(err.message.contains("trailing") || err.message.contains("operator"));
    }

    #[test]
    fn round_trip_through_serde() {
        // Compiled filter should serialize back to our existing JSON DSL so
        // the engine and the gateway use one wire format end-to-end.
        let f = parse("category == 'books' && price >= 10");
        let json = serde_json::to_value(&f).unwrap();
        assert_eq!(
            json,
            json!({
                "must": [
                    {"key": "category", "match": {"value": "books"}},
                    {"key": "price", "range": {"gte": 10.0}}
                ]
            })
        );
    }
}
