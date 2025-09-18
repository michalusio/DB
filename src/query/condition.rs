use std::{fmt::{Display, Write}, iter, collections::BTreeSet};

use crate::objects::ObjectField;

use super::{unary_expression::UnaryExpression, binary_expression::BinaryExpression};

pub trait Normalizable {
    fn normalize(self) -> Condition;
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Condition {
    Or(BTreeSet<Condition>),
    And(BTreeSet<Condition>),
    
    Unary(UnaryExpression),
    Binary(BinaryExpression),

    Column(&'static str),
    Value(ObjectField)
}

impl Normalizable for Condition {
    fn normalize(self) -> Condition {
        match self {
            Condition::Or(conditions) => {
                let normalized_items: Vec<Condition> = conditions.into_iter().map(Normalizable::normalize).collect();
                let (ors, mut rest): (Vec<_>, Vec<_>) = normalized_items.into_iter()
                    .partition(|i| matches!(i, Condition::Or(_)));
                rest.extend(ors.into_iter().flat_map(|a| match a {
                    Condition::Or(c) => c,
                    _ => unreachable!()
                }));

                let exploded = rest
                    .into_iter()
                    .map(|a| match a {
                        Condition::And(c) => c,
                        _ => {
                            let mut set = BTreeSet::new();
                            set.insert(a);
                            set
                        }
                    })
                    .fold(vec![], |curr: Vec<BTreeSet<Condition>>, next| {
                        let mut result: Vec<BTreeSet<Condition>> = vec![];
                        for n in next {
                            if curr.is_empty() {
                                result.push(iter::once(n).collect());
                            } else {
                                for c in &curr {
                                    result.push(c.iter().chain(iter::once(&n)).cloned().collect());
                                }
                            }
                        }
                        result
                    })
                    .into_iter()
                    .map(Condition::Or)
                    .collect();

                Condition::And(exploded)
            },
            Condition::And(conditions) => {
                let (ands, mut rest): (BTreeSet<_>, BTreeSet<_>) = conditions
                    .into_iter()
                    .map(Normalizable::normalize)
                    .partition(|i| matches!(i, Condition::And(_)));
                rest.extend(ands.into_iter().flat_map(|a| match a {
                    Condition::And(c) => c,
                    _ => unreachable!()
                }));
                Condition::And(rest)
            },
            Condition::Unary(u) => u.normalize(),
            Condition::Binary(expression) => expression.normalize(),

            Condition::Value(_) => self,
            Condition::Column(_) => self,
        }
    }
}

impl Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Condition::And(c) => {
                for (index, cond) in c.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" AND ")?;
                    }
                    match cond {
                        Condition::Or(_) | Condition::And(_) => {
                            f.write_char('(')?;
                            cond.fmt(f)?;
                            f.write_char(')')?;
                        },
                        _ => cond.fmt(f)?,
                    }
                }
                Ok(())
            },
            Condition::Or(c) => {
                for (index, cond) in c.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" OR ")?;
                    }
                    match cond {
                        Condition::Or(_) | Condition::And(_) => {
                            f.write_char('(')?;
                            cond.fmt(f)?;
                            f.write_char(')')?;
                        },
                        _ => cond.fmt(f)?,
                    }
                }
                Ok(())
            },
            Condition::Unary(expr) => expr.fmt(f),
            Condition::Column(s) => f.write_str(s),
            Condition::Binary(expr) => expr.fmt(f),
            Condition::Value(value) => value.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Condition;
    use super::Normalizable;
    use crate::set;
    use crate::query::binary_expression::BinaryExpression;
    use crate::query::binary_expression::BinaryExpressionType;
    use crate::query::unary_expression::UnaryExpression;
    use crate::query::unary_expression::UnaryExpressionType;

    #[test]
    fn condition_normalization_test_not_equal() {
        let c = Condition::And(
            set![
                Condition::Binary(
                    BinaryExpression {
                        expression_type: BinaryExpressionType::NotEqualTo,
                        first: Condition::Column("a").into(),
                        second: Condition::Column("b").into()
                    }
                )
            ]
        );

        assert_eq!(c.to_string(), "a != b");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "NOT (a == b)");
    }

    #[test]
    fn condition_normalization_test_double_negation() {
        let c = Condition::Unary(
            UnaryExpression {
                expression_type: UnaryExpressionType::Not,
                term: Box::new(Condition::Unary(UnaryExpression {
                    expression_type: UnaryExpressionType::Not,
                    term: Condition::Column("a").into(),
                }))
            }
        );

        assert_eq!(c.to_string(), "NOT (NOT (a))");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "a");
    }

    #[test]
    fn condition_normalization_test_or_demorgan() {
        let c = Condition::Unary(
            UnaryExpression {
                expression_type: UnaryExpressionType::Not,
                term: Box::new(Condition::Or(set![
                    Condition::Column("a"),
                    Condition::Column("b"),
                    Condition::Column("c"),
                ]))
            }
        );

        assert_eq!(c.to_string(), "NOT (a OR b OR c)");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "NOT (a) AND NOT (b) AND NOT (c)");
    }

    #[test]
    fn condition_normalization_test_and_demorgan() {
        let c = Condition::Unary(
            UnaryExpression {
                expression_type: UnaryExpressionType::Not,
                term: Box::new(Condition::And(set![
                    Condition::Column("a"),
                    Condition::Column("b"),
                    Condition::Column("c"),
                ]))
            }
        );

        assert_eq!(c.to_string(), "NOT (a AND b AND c)");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "NOT (a) OR NOT (b) OR NOT (c)");
    }

    #[test]
    fn condition_normalization_test_and() {
        let c = Condition::And(
            set![
                Condition::Binary(
                    BinaryExpression {
                        expression_type: BinaryExpressionType::EqualTo,
                        first: Condition::Column("a").into(),
                        second: Condition::Column("b").into()
                    }
                ),
                Condition::Binary(
                    BinaryExpression {
                        expression_type: BinaryExpressionType::LessThan,
                        first: Condition::Column("c").into(),
                        second: Condition::Column("d").into()
                    }
                )
            ]
        );

        assert_eq!(c.to_string(), "a == b AND c < d");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "a == b AND c < d");
    }

    #[test]
    fn condition_normalization_test_or_and() {
        let c = Condition::And(
            set![
                Condition::Binary(
                    BinaryExpression {
                        expression_type: BinaryExpressionType::EqualTo,
                        first: Condition::Column("a").into(),
                        second: Condition::Column("b").into()
                    }
                ),
                Condition::Or(
                    set![
                        Condition::Binary(
                            BinaryExpression {
                                expression_type: BinaryExpressionType::LessThan,
                                first: Condition::Column("c").into(),
                                second: Condition::Column("d").into()
                            }
                        ),
                        Condition::Binary(
                            BinaryExpression {
                                expression_type: BinaryExpressionType::GreaterThan,
                                first: Condition::Column("f").into(),
                                second: Condition::Column("g").into()
                            }
                        )
                    ]
                )
                
            ]
        );

        assert_eq!(c.to_string(), "(c < d OR f > g) AND a == b");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "(c < d OR g < f) AND a == b");
    }

    #[test]
    fn condition_normalization_test_and_or() {
        let c = Condition::Or(
            set![
                Condition::Binary(
                    BinaryExpression {
                        expression_type: BinaryExpressionType::EqualTo,
                        first: Condition::Column("a").into(),
                        second: Condition::Column("b").into()
                    }
                ),
                Condition::And(
                    set![
                        Condition::Binary(
                            BinaryExpression {
                                expression_type: BinaryExpressionType::LessThan,
                                first: Condition::Column("c").into(),
                                second: Condition::Column("d").into()
                            }
                        ),
                        Condition::Binary(
                            BinaryExpression {
                                expression_type: BinaryExpressionType::GreaterThan,
                                first: Condition::Column("f").into(),
                                second: Condition::Column("g").into()
                            }
                        )
                    ]
                )
                
            ]
        );

        assert_eq!(c.to_string(), "(c < d AND f > g) OR a == b");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "(a == b OR c < d) AND (a == b OR g < f)");
    }

    #[test]
    fn condition_normalization_test_and_or_three() {
        let c = Condition::Or(
            set![
                Condition::Column("a"),
                Condition::And(
                    set![
                        Condition::Column("b"),
                        Condition::Column("c")
                    ]
                ),
                Condition::Column("d")
            ]
        );

        assert_eq!(c.to_string(), "(b AND c) OR a OR d");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "(a OR b OR d) AND (a OR c OR d)");
    }

    #[test]
    fn condition_normalization_test_full() {
        let c = Condition::Unary(UnaryExpression {
            expression_type: UnaryExpressionType::Not,
            term: Box::new(
                Condition::Or(
                    set![
                        Condition::Column("a"),
                        Condition::And(
                            set![
                                Condition::Column("b"),
                                Condition::Column("c")
                            ]
                        ),
                        Condition::Column("d")
                    ]
                )
            )
        });

        assert_eq!(c.to_string(), "NOT ((b AND c) OR a OR d)");

        let normalized = c.normalize();

        assert_eq!(normalized.to_string(), "(NOT (b) OR NOT (c)) AND NOT (a) AND NOT (d)");
    }
}