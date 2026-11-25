# Automatic parallel partition reads

Generated table scans of an attached **read-only** catalog automatically attempt
ADBC partitioned execution in autocommit. The driver supplies the partitions and
DuckDB schedules the readers; no scanner-specific thread setting is needed:

```sql
ATTACH '<uri>' AS remote (
    TYPE adbc,
    driver '<driver>',
    READ_ONLY
);
SELECT * FROM remote.schema_name.table_name;
```

Only generated attached-table scans participate. Whole-query pushdown stays
enabled by default. When the optimizer chooses it, the existing remote query
path runs; if a query needs generated table scans instead, those scans can use
partitions. Setting `query_pushdown false` remains an independent way to request
local query processing, but is not required for partitioning.

Read-write attachments and scans in explicit transactions use the existing
serial execution path. Eligibility is checked at execution time, so the same
prepared query can use serial execution inside `BEGIN` and partitioned execution
in autocommit. Arbitrary `adbc_scan(database, query)`, direct
`adbc_scan_table(...)`, remote whole-query pushdown and prefetched command results
remain on their established paths.

Pipelines that can stop consuming early (for example streaming LIMIT or limited
DISTINCT), or have no available pipeline context, admit **one** reader. Normal
LIMIT completion does not necessarily interrupt the client context, so starting
blocked peers there would be unsafe. This is an intentional initial eligibility
limit, not a claim that every scan uses all available threads. Full-consumption
scan/join/aggregate pipelines may use concurrent readers.

Global initialization discovers the partitions before DuckDB calls `MaxThreads()`.
The scan reports the partition count (with a floor of one for empty results), and
DuckDB applies its scheduler thread limit and pipeline constraints. Workers open
connections only when they claim work. The number of concurrent readers is at most
the smaller of the partition count and DuckDB's `threads` setting; a pipeline may
use fewer. Each active reader adds a remote connection for that scan. Partition
count and any driver-specific partitioning options remain the driver's concern.

## Execution and ownership

Binding still uses schema/catalog metadata without executing a query. At runtime
the scanner builds the same projected SELECT, quoted identifiers and bound filter
parameters once during global initialization, then calls ADBC `ExecutePartitions`
once for eligible scans. It retains the producer statement and shared initialized
database, and copies opaque descriptor bytes
before releasing the driver's descriptor container. The implementation refuses
more than 65,536 descriptors or more than 64 MiB of descriptor bytes. A successful
zero-partition set with a valid schema is an empty result.

Each DuckDB local worker owns a different ADBC connection and operation lease,
and reads one claimed partition stream at a time. Each descriptor index is
claimed exactly once. No SQL is re-executed per worker and no Arrow stream's
`get_next` is called concurrently. Connections originate from the same initialized
database, preserving its driver configuration, TLS and authentication settings;
private descriptor contents and credentials are never added to scan diagnostics.
ADBC partition descriptors must support reading on independent connections;
connection-local session objects are not shared with reader connections.

Each stream has local schema/Arrow conversion state and must match the schema
returned by `ExecutePartitions`, including nested/dictionary structure and
metadata. Existing filter/projection handling remains. Partition scans do not
promise physical result order; use SQL `ORDER BY` where order matters. Independent
table SELECTs keep their existing independent snapshot semantics: this does not
create a common cross-table snapshot. A driver that captures a whole query before
partitioning its finished output may only parallelize downloading that result;
this feature does not make its query computation parallel.

## Errors and cancellation

Only an initial `NOT_IMPLEMENTED` with entirely untouched schema/partition outputs
may fall back once to the original `ExecuteQuery`. Successful descriptor exposure,
invalid output, authentication/I/O errors, schema mismatches and later read errors
never retry or replay the query. A failure from any reader fails the query. The
first observed source exception is retained and rethrown by peers, so cancellation
does not replace it with a generic peer-failure message.

Arrow `get_next` returning zero with a released array is successful end-of-stream.
The scanner calls `get_last_error` only after a failed operation, as required by
the Arrow C stream interface; an empty or stale driver error buffer cannot turn
successful completion into a read error.

The producer's statement watcher covers partition execution. Reader watchers call
**connection** cancellation, because `ReadPartition` has no statement, on query
interrupt or a peer failure. Each watcher retains its connection and is stopped
before releasing that stream/lease. Worker state must be destroyed before producer
and database state. Cancellation support is driver-dependent; no unsafe concurrent
stream release or forced connection destruction substitutes for a driver that
cannot interrupt a blocked call. Opening/initializing a connection precedes its
reader watcher: a blocked `ConnectionInit` is a separate connect-time limitation,
not covered by the initialized-reader cancellation guarantee. No cancellation is
issued on an uninitialized connection.

## Testing

The Python regression suite exercises the loadable extension with the optimizer
enabled. It covers automatic selection without attachment tuning, preservation of
whole-query pushdown, prepared queries across transaction boundaries, scheduler
and partition-count limits, NULL/exact decimal results, descriptor copy ownership,
filter binding/projection, EOF handling, empty results, failure without replay,
malformed/over-budget descriptors and cleanup counters.

The early-stop test uses `preserve_insertion_order=false` plus `LIMIT 1`: partition
zero yields rows while peers would block until cancellation. Only the first reader
is admitted. A separate full-consumption test asserts at least two simultaneous
independent streams. Cancel tests separately block `ReadPartition` and `get_next`,
while a gated peer-failure test preserves its original source error. Counters check
producer, database, connection and stream cleanup after LIMIT, error and interrupt.

The **ADBC Driver Tests** workflow runs these Python tests in addition to the SQL
driver tests. See [Build and test](BUILDING.md) to run them locally with the matching
DuckDB version. The partition fixtures are synthetic drivers; they do not start
Redis or claim real-driver performance improvements.
