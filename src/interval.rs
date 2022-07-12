// This can't just be Option<u64> because we need Incomplete ordered last.
#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum IntervalEnd {
    Complete(u64),
    Incomplete
}

#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Interval {
    pub start: u64,
    pub end: IntervalEnd,
}
