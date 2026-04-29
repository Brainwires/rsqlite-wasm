use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("SQL parse error: {0}")]
    Syntax(String),

    #[error("unsupported SQL feature: {0}")]
    Unsupported(String),

    #[error("semantic error: {0}")]
    Semantic(String),
}

impl From<sqlparser::parser::ParserError> for ParseError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        ParseError::Syntax(e.to_string())
    }
}
