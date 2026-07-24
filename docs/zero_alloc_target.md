# Zero-alloc target: a catalog of C++/Seastar allocation-reduction strategies

General techniques for driving a C++/Seastar hot path's allocations/op toward zero. Nothing here is
specific to graph engines -- each entry is a language/runtime-level pattern, stated generally, with a
ragedb hot path attached as the worked case. Add entries as techniques land; cite the commit that proved
the win.

**Status: this catalog is a TARGET, not a finished campaign.** ragedb has not yet run a dedicated
allocation-reduction pass. Each entry below names a real ragedb hot path and the cure; each stays a
**hypothesis** until a counting-shim A/B proves it and the proving commit is cited here. Treat the
"candidate"/greenfield labels seriously -- see *Floors are hypotheses* below.

## First, the measurement discipline

1. **Count before theorizing.** Override global `operator new`/`operator new[]` (and the sized-delete
   pair) with an `std::atomic<uint64_t>` counting shim compiled into a scratch harness; arm the counter
   around exactly the region under test (parse, optimize, or one executed query) and read the delta.
   Seastar also exposes per-shard `seastar::memory::stats()` (`mallocs()`/`frees()`) -- diff it across a
   warm run for an allocator-level cross-check. Code-reading theories are wrong often enough to be
   expensive; attribute to a line with `heaptrack` or jemalloc's `prof`, not by staring.
2. **Trust counts and wall A/B, not macOS CPU profiles.** Per the repo's performance rule, macOS C++ CPU
   profiles wildly inflate syscall frames. Allocation counts are deterministic; wall-clock is the product
   for a perf claim and needs the quiet-box gate (`taskman lock run -max-load N`). Never publish a timing
   from a run that did not hold the lock and pass `-max-load`.
3. **Measure the steady state.** Warm the run first -- schema load, plan/query cache, any lazy index --
   then count ONE warm pass, so one-time initialization does not masquerade as per-op cost.
4. **A/B each step independently, same box.** `git stash push -- <files>` for the baseline leg, pop for
   the candidate. A two-part fix can hide a regression in one half; split it.
5. **Guard behavior with a result-identity oracle.** Fast paths must be provably output-identical to the
   general path. ragedb has already been bitten here: a far-node-blind degree rewrite silently
   mis-answered a constrained `COUNT{}`, and an anonymous destination's property-map was pruned before
   its own filter ran -> silent 0 rows. Diff against the LDBC reference results after every step.
6. **Pin the win.** Once the counting shim exists, bound allocs/row on the hot paths in a test with ~2x
   headroom. A later change that re-adds a per-row allocation then fails CI instead of landing silently.

## Where C++/Seastar allocates, and what to do about it

### 1. Per-row binding maps in the executor (the elephant)

`GqlRow::bindings` is a `std::map<std::string, GqlValue>` (`src/gql/GqlValue.h:73`) -- a red-black tree
with a heap-allocated node **per entry**, ordered by a `std::string` key. Every candidate row is a full
deep copy of that tree: `GqlRow new_row = base_row;` runs once per start node and once per expansion
(`src/gql/executor/PathTraverser.cpp:1260, 1305`). An N-hop pattern over a wide row copies a whole tree
of heap nodes + key strings per intermediate row. This is almost certainly the single largest allocation
source in query execution. Cures, in rough order of effort:

- **Reserve/reuse, move not copy.** Where a row is extended and the parent is dead, `std::move` the
  bindings instead of copying; carry a scratch row reused across a step (see entry 2).
- **Flat small-map.** Replace the tree with a sorted `boost::container::small_vector<pair<Key,Value>>`
  (or `absl::flat_hash_map`): one backing allocation, inline storage for the common handful of bindings,
  probe linearly/by binary search. A row with <= 8 bindings then costs zero heap warm.
- **Intern keys to slot ids (the real fix).** Assign every pattern variable / projected name a dense
  integer slot at plan time; the row becomes a `std::vector<GqlValue>` indexed by slot, not a
  string-keyed map. No key strings, no tree, no per-entry node -- one contiguous allocation per row,
  reservable to the known width. This is the structural end state; the flat-map is the incremental step.
- **Structural sharing / copy-on-extend.** If most bindings are unchanged across a hop, a persistent map
  or a parent-pointer frame shares the prefix and only allocates the delta.

### 2. Scratch allocated per call / per expansion

**Hoist + reset is the master pattern:** `vec.clear()` keeps capacity; reuse the neighbor/expansion
buffer across traversal steps rather than allocating a fresh `std::vector<GqlRow>` per step; reuse
group-key buffers across aggregator rows. Ownership lives with the caller (`do_with` a scratch struct
that outlives the loop) because generic code cannot reset an opaque value. Convert reslicing-style queue
walks to a head-index walk over a reused buffer for BFS/queue kernels. Presize (`reserve`) whenever a
length or upper bound is known -- vector growth is O(log n) reallocations you do not need to pay.

### 3. Seastar continuation & coroutine frames (the C++-specific one)

Every `.then([...](auto x){...})` may heap-allocate a **continuation object** sized to its capture, and
every `co_await` allocates a **coroutine frame**. One future-chain hop per row is death by a thousand
cuts. Cures:

- **Batch the seam.** Traverse a whole step into one `std::vector<GqlRow>` and attach ONE continuation,
  not a future per row. ragedb already drives streamed traversals with *bounded* concurrency and
  parallelizes inner steps -- that also bounds the number of simultaneously-live frames; keep new hot
  paths inside that structure rather than spawning a future per element.
- **Return ready futures.** When the value is already in hand, `make_ready_future(...)` avoids scheduling
  a continuation at all; prefer it over a `.then` that only forwards.
- **Keep captures small.** Capture a single `lw_shared_ptr` to a state struct, not a dozen values -- a
  large capture is a large continuation allocation. (See entry 4 on `lw_shared_ptr`.)
- **Loop, don't recurse per element.** `seastar::repeat` / `do_until` with `maybe_yield` expresses an
  async loop with reused state instead of a per-iteration continuation graph.
- Caveat: never trade a reactor stall for an allocation. Collapsing async into a synchronous loop that
  runs long enough to stall the shard swaps a cheap `malloc` for a latency spike -- measure both.

### 4. `shared_ptr` control blocks and atomic refcount traffic

`std::shared_ptr` heap-allocates a control block (two allocations unless `make_shared` fuses it into one)
and every copy is an **atomic** increment/decrement. In Seastar's shared-nothing, per-shard world the
atomic is almost always unnecessary:

- **Prefer `seastar::lw_shared_ptr`** (non-atomic, single-shard) for per-query state that never crosses
  a shard -- cheaper refcounting than `std::shared_ptr`. ragedb's GQL layer currently uses `std::`
  variants throughout (no `lw_shared_ptr` yet: greenfield).
- **`make_shared`/`make_lw_shared`** to fuse the control block with the object.
- **Pass by `const&`, not by value.** A `shared_ptr` taken by value per row is an atomic pair per row for
  nothing; one shared state struct per query (the `shared_rows` pattern already used in the correlated
  precompute) beats many small shared pointers copied around.

### 5. Small buffers, type-erasure, and `std::function`

- **`std::function` type-erases and may heap-allocate** its target. `GqlRowSink::consume` is a
  `std::function<seastar::future<>(std::vector<GqlRow>)>` (`src/gql/executor/PathTraverser.h:107`) --
  fine at API granularity (one per query), a problem only if constructed per row. Prefer
  `seastar::noncopyable_function` (no allocation for movable targets) or a concrete template parameter on
  the hot seams.
- **Inline the small-N case.** `boost::container::small_vector<T, N>` / `seastar::circular_buffer` keep
  the common handful of elements inline and spill to the heap only on overflow.

### 6. Boxing rows/results per element -- build survivors only

- **Key-only top-k.** For `ORDER BY ... LIMIT k`, evaluate the sort KEY alone per candidate and construct
  the full projected row only for the ~k the heap keeps. ragedb already carries a LIMIT stop-protocol
  (do not over-scan); extend it so a bounded top-k never materializes the losers.
- **Flat backing for rows-of-lists.** Variable-length relationship lists and per-group aggregation state
  want one flat `n*width` backing with `(offset, len)` windows, turning n per-row vectors into one
  allocation.
- **Move survivors into the output form at the last moment**, keeping candidates as plain structs until
  the truncation.

### 7. Per-query arenas (`std::pmr`)

Back per-query transient scratch (intermediate rows, temp vectors, formatted key strings) with a
`std::pmr::monotonic_buffer_resource`: bump-allocate, then drop the whole arena at query end -- no
per-object `free`, no fragmentation. Seastar's allocator is already per-shard; a pmr arena on top removes
per-object churn for everything that dies at end-of-query. Pair with `std::pmr::vector` / a pmr flat-map
for the row bindings (entry 1). No `pmr` usage exists in `src/gql` yet: greenfield.

### 8. Recomputing constants / formatting per element

- **Hoist per-row constants to per-query.** The correlated precompute builds a binding key with
  `"_count_" + std::to_string(sid)` (`src/gql/GqlExecutor.cpp`) -- a fresh `std::string` per row per
  subquery. The key is constant across rows: format it once per subquery and reuse, or (entry 1) bind by
  integer slot so no string is built at all.
- **`std::to_chars` into a reused buffer**, never `std::to_string`/`ostringstream`/`fmt` in a per-row
  loop (each allocates). This mirrors the repo rule "`strconv.Append*` into a reused `[]byte`."
- **Const-fold and memoize.** A scalar function of constant arguments should fold at plan time, not per
  row; a constant `zoned_datetime('2011-12-01')` should be parsed once, not per visited row (LDBC bi/q1
  evaluates exactly this in a hot filter). Memoize keyed on the call site, stored on per-execution state
  -- never on a shared cached plan (a mutable cache on a shared AST is a re-entrancy bug).
- **Derive monomorphic compare fast paths once per compile** (resolve column/offset for a
  property-vs-literal compare at plan time, not per row).

### 9. Bulk construction over incremental insert

Compressed/structured containers (Roaring bitmaps, sorted indexes) pay per-insert container management.
Collect keys into a plain `std::vector`, sort, and construct once. Roaring's `addRange`/bulk path also
produces the run containers the incremental path never creates, so a bitmap built incrementally may not
be byte-identical to a bulk-built one -- a conformance gotcha worth knowing when comparing serialized
bitmaps.

## Covered for free in C++ (no action needed)

- **"Reslicing loses the backing"**: `std::vector::clear()` keeps capacity by default; the C++ hazard is
  the reverse -- an accidental copy where a move/reference was intended. Watch `auto x = row;` and
  by-value range-`for` over heavy elements.
- **GC-relief object pools**: there is no GC to relieve; the C++ analogue is the arena (entry 7) and
  plain reuse (entry 2), chosen for lifetime, not collector pressure.

## Anti-patterns and honest labels

- **Don't move cost -- label it.** Reusing scratch across calls is a real reduction; relocating
  computation into an untimed setup phase changes what the number means. If a change moves work, the
  commit must say so.
- **Floors are hypotheses.** Declare a floor only alongside the shim/A-B line proving the residual is
  structural -- and expect to be wrong. A "genuine floor" declared from code-reading almost always falls
  when re-challenged with a fresh count.
- **No workload recognizers.** A change that helps only because the code knows WHICH query is running is
  overfitting. The test: would an unseen query of the same shape benefit?
- **Never stall the reactor to save an allocation.** In Seastar the allocation win is worthless if it
  introduces a reactor stall; an allocation reduction and a latency regression can hide in the same diff.
  Gate perf claims on wall-clock under `-max-load`, not on the allocation count alone.
