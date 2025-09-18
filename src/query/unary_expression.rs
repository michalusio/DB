use std::fmt::{Display, Write};

use super::condition::{Condition, Normalizable};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnaryExpression {
    pub expression_type: UnaryExpressionType,
    pub term: Box<Condition>
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnaryExpressionType {
    Length,
    Not
}

impl Normalizable for UnaryExpression {
    fn normalize(self) -> Condition {
        match self.expression_type {
            UnaryExpressionType::Length => Condition::Unary(UnaryExpression{
                expression_type: UnaryExpressionType::Length,
                term: Box::new(self.term.normalize())
            }),
            UnaryExpressionType::Not => match *self.term {

                Condition::Unary(UnaryExpression{
                    expression_type: UnaryExpressionType::Not,
                    term: t
                }) => t.normalize(),

                Condition::Or(conditions) => {
                    Condition::And(
                        conditions.into_iter()
                        .map(|c| UnaryExpression {
                            expression_type: UnaryExpressionType::Not,
                            term: Box::new(c)
                        }.normalize())
                        .collect()
                    )
                },

                Condition::And(conditions) => {
                    Condition::Or(
                        conditions.into_iter()
                        .map(|c| UnaryExpression {
                            expression_type: UnaryExpressionType::Not,
                            term: Box::new(c)
                        }.normalize())
                        .collect()
                    )
                },

                _ => Condition::Unary(UnaryExpression{
                    expression_type: UnaryExpressionType::Not,
                    term: Box::new(self.term.normalize())
                })
            },
        }
    }
}

impl Display for UnaryExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.expression_type {
            UnaryExpressionType::Length => f.write_str("LEN")?,
            UnaryExpressionType::Not => f.write_str("NOT ")?
        }
        f.write_char('(')?;
        self.term.fmt(f)?;
        f.write_char(')')
    }
}