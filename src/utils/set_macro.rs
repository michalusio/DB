#[macro_export]
macro_rules! set {
    () => (
        std::collections::BTreeSet::new()
    );
    ($($x:expr),+ $(,)?) => ({
        let mut set = std::collections::BTreeSet::new();
        set.extend([$($x),+]);
        set
    });
}