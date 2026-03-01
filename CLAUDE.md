# zola_db

A kdb-inspired time-series database backed by mmap-ed Arrow date partitions.

## Typical deployment

| Dimension | Value |
|---|---|
| Symbols | 500 |
| History | 5 years |
| Value columns per table | 5 |
| Rows per partition | 1,000,000,000 |
| Typical query rows | ~100,000 (uniformly distributed over full history) |

## Performance vs complexity

Think carefully about whether performance improvements are worth the complexity. Don't add complexity for trivial gains.

## Panics vs errors

We use panics for unexpected cases, like invalid data on-disk. We use errors for expected cases, like when the user provides invalid input.
