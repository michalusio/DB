use std::fmt::{Display, Write};

use super::{condition::{Condition, Normalizable}, unary_expression::{UnaryExpressionType, UnaryExpression}};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BinaryExpression {
    pub expression_type: BinaryExpressionType,
    pub first: Box<Condition>,
    pub second: Box<Condition>
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BinaryExpressionType {
    EqualTo,
    NotEqualTo,
    LessThan,
    LessThanOrEqualTo,
    GreaterThan,
    GreaterThanOrEqualTo,
    Like,
    NotLike
}

impl Normalizable for BinaryExpression {
    fn normalize(self) -> Condition {
        match self.expression_type {
            BinaryExpressionType::EqualTo => Condition::Binary(self),
            BinaryExpressionType::LessThan => Condition::Binary(self),
            BinaryExpressionType::LessThanOrEqualTo => Condition::Binary(self),
            BinaryExpressionType::Like => Condition::Binary(self),


            BinaryExpressionType::NotLike => Condition::Unary(UnaryExpression {
                expression_type: UnaryExpressionType::Not,
                term: Condition::Binary(BinaryExpression {
                    expression_type: BinaryExpressionType::Like,
                    first: self.first,
                    second: self.second
                }).into()
            }),
            BinaryExpressionType::NotEqualTo => Condition::Unary(UnaryExpression {
                expression_type: UnaryExpressionType::Not,
                term: Condition::Binary(BinaryExpression {
                    expression_type: BinaryExpressionType::EqualTo,
                    first: self.first,
                    second: self.second
                }).into()
            }),
            BinaryExpressionType::GreaterThan => Condition::Binary(BinaryExpression {
                expression_type: BinaryExpressionType::LessThan,
                first: self.second,
                second: self.first
            }),
            BinaryExpressionType::GreaterThanOrEqualTo => Condition::Binary(BinaryExpression {
                expression_type: BinaryExpressionType::LessThanOrEqualTo,
                first: self.second,
                second: self.first
            }),
        }
    }
}

impl Display for BinaryExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.first.as_ref() {
            Condition::Or(_) | Condition::And(_) | Condition::Binary(_) | Condition::Unary(_) => {
                f.write_char('(')?;
                self.first.fmt(f)?;
                f.write_char(')')?;
            },
            _ => self.first.fmt(f)?,
        }

        match self.expression_type {
            BinaryExpressionType::EqualTo => f.write_str(" == ")?,
            BinaryExpressionType::NotEqualTo => f.write_str(" != ")?,
            BinaryExpressionType::LessThan => f.write_str(" < ")?,
            BinaryExpressionType::LessThanOrEqualTo => f.write_str(" <= ")?,
            BinaryExpressionType::GreaterThan => f.write_str(" > ")?,
            BinaryExpressionType::GreaterThanOrEqualTo => f.write_str(" >= ")?,
            BinaryExpressionType::Like => f.write_str(" LIKE ")?,
            BinaryExpressionType::NotLike => f.write_str(" NOT LIKE ")?,
        }

        match self.second.as_ref() {
            Condition::Or(_) | Condition::And(_) | Condition::Binary(_) | Condition::Unary(_) => {
                f.write_char('(')?;
                self.second.fmt(f)?;
                f.write_char(')')?;
            },
            _ => self.second.fmt(f)?,
        }
        Ok(())
    }
}