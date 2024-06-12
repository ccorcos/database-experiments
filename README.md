# Database Experiments

This repo contains a set of self-contained experiments in building application database abstractions in JavaScript/TypeScript.

## Goal / Requirements

At a high level, I just want it to be a lot easier to build applications, and in particular, applications like Notion.

1. Queries must have realtime reactivity on the client.
2. Clients must be able to work offline.
3. All queries must be indexable.

	In SQL, for example, you can't index columns that are joined across tables. You need to set up triggers and manually denormalize data yourself, poluting your schema.

	In Notion, users can construct their own schemas and create views that filter data using those schemas. I want to be able to dynamically create indexes for these queries.

4. The same (or similar) abstraction can be used in the client and the server.

	Apps like Notion have 3 databases -- one on the backend and two on the frontend. On the frontend, one database is a synchronous in-memory cache, while the other is an asynchronous durable cache.

	When an application needs to work offline with optimistic updates, then you end up wanting to run the same queries from the backend but against your frontend caches.


# Navigating Trade-offs

## Append-only log / CRDT vs record versions / operational transforms

There are 2-ish different approaches to sync.

1. Append-only log of writes and sync that log to clients.

	- Simple sync logic.
	- Causal consistency with vector clocks is complicated, leads to scaling challenges with ephemeral web users.
	- Separate log for separate permission contexts. Lots of fan-out, data duplication. Storage-heavy.
	- Minimal number of subscriptions.
	- Can't exactly delete data. Deletes need to be preserved in the log.
	- Great offline behavior.
	- Need to download the entire log, slow first-load.
	- Doesn't scale well for large collaborative apps.
	- Works for P2P applications, but often lose transactional constraint guarantees like "every username needs to be unique".
	- Existing implementations: Automerge and Y.js.
	- Can't really reject a transaction. Need to write and then amend.

2. Every record has a version key, sync by comparing versions.

	- Slightly less simple sync logic, but still pretty simple.
	- Lots of subscriptions per client.
	- Less data storage, but more fetching.
	- First load to ephemeral users is really fast.
	- Scales to very large workspaces, allows partial download / caching.
	- Data can actually be deletes.
	- Both Notion and Linear use this approach successfully in production.

Given the things I want to build, I'm going with the second approach. It maintains the most flexibility going forward — you can implement (1) on top of (2) if you want.


## Global versionstamps vs per-record versions

There are 2 approaches here that define the extremes of sharded vs non-sharded.

1. DenoKV uses `ulid` for global incrementing transaction id versions.
2. Notion uses per-record versions that increment.

- Global versionstamps relies on the unique architecture of FoundationDb to work well. This doesn't necessarily work well for Postgres.
- With versionstamps, you can write a record without having to read it first.
- Record versions is basically a sharded versionstamp and could distribute and scale better.
- With record versions, you need to soft-delete keys so you don't lose the version number.

I feel like there's still a solution that incorporates a little of both approaches.

- servers generate versionstamps.
- use the server-generated versionstamp if its larger than the existing record, or increment the existing record versionstamp.

This means that you can delete a record and so long as it's not overwritten in the same millisecond, then you're not going to have any issues. But it's possible that in the same millisecond, a delete is followed up by a write with a lower versionstamp which wouldn't be good.

We can solve that in terms of sync logic actually. There are two components to the version: {timestamp, random}. If the timestamp is the same, then we compare if the random bytes are equal. This means that you may have to re-fetch your own write if it was concurrent with other write in the same millisecond. This isn't going to happen too often though and should be fine.

The only concern here is the coordination of timestamps between servers. Can we rely on servers to have the same timestamps? Well, we can actually use Postgres for this.

> So where did we end up?

We can use `ulid` (some something similar) still for record versions. In Postgres, we'll have to generate the timestamp in the database. On the frontend, we compare the timestamp-component of the ulid to see if we have the latest version, and only if they're equal, we compare the randomness just for equality.

## Schemaless, Transactional

Not much to say here.

The schema should be application-enforeced rather than database-enforced. There's lots of articles about why schemaless is better / more performant. When databases scale up, scheams become an expensive burden.

Transactional gives you a lot of flexibility with what you want to do. I like the explicit version checks with DenoKV so that transactionality is optional.

However, when syncing offline writes from a client to a server, then we lose transactionality gaurantees between records. But this is a trade-off that needs to be manages. If we need transacitonal writes across records, then you will need to be online to make that request.

## Lists

There are 2-ish approaches here. If there are more approaches, please let me know!

1. Lists are vectors with a sort key. Typically using fractional-indexing.

	- index values can grow pretty fast if you keep inserting into the middle
	- merging offline conccurent collaborative writes will interleave values
	- when indexes land on the same value, you need some special logic to spread them out again.
	- Can efficently store and retrieve in a btree index.

2. Linked lists.

	- Need to transactionally write across multiple records to manipulate pointers.
	- Can't really delete an item ever otherwise you break the list.
	- Simple mental model — insert X after Y.
	- More expensive recursive query to read.
	- Expensive to determine if an item is part of a list.
	- Doens't interleave offline concurrent writes.
	- Harder to build compound btree indexes with.


I'm not in love with fractional indexing, but it does seem like the best approach so far. Being able to lay out a list in order in a btree seems to makes a lot of sense to me.

Some more reading TODO:

- https://www.youtube.com/watch?si=8NQ_6xW46F80LmjU&t=808&v=Mr0a5KyD6BU&feature=youtu.be
- https://www.bartoszsypytkowski.com/yata/
- [Consider biasing the algorithm to work better here](https://github.com/rocicorp/fractional-indexing/blob/785c6f5a4451d6552608274cbb397cea45e547aa/src/index.js#L250)


I haven't made a concrete decision here yet.


# Data Structures


## KeyValueDatabase (kv.ts)

This is a lightweight abstraction on top of a hash-map with record versions for concurrency gaurantees.

```ts
type KeyValueDatabase<V> = {
	get(key: string): { value: V; version: string } | undefined
	write(tx: {
		check?: { key: string; version: string | undefined }[]
		set?: { key: string; value: V }[]
		delete?: string[]
	}): void
}
```

When dealing with async code, these concurrency checks are more important.

## OrderedKeyValueDatabase (okv.ts)

This is an abstraction on top of a sorted array, using binary search to index into the array.

```ts
type OrderedKeyValueDatabase = {
	get(key: string): { value: any; version: string } | undefined

	/** start is inclusive. end is exclusive. prefix is exclusive */
	list = (args: {
		prefix?: string
		start?: string
		end?: string
		limit?: number
		reverse?: boolean
	}): { value: any; version: string }[]

	write(tx: {
		check?: { key: string; version: string }[]
		// TODO: check range
		set?: { key: string; value: any }[]
		delete?: string[]
		// TODO: delete range
	}): void
}
```

Actual ordered key-value databases often use log-sorted merge trees. And general purpose databases like SQLite and Postgres use B+ trees to store sorted indexes.

## OrderedTupleValueDatabase (tuple-okv.ts)

Using the `lexicodec` library, we can encode tuples into strings. But if we aren't persisting data, we don't want to have to pay the cost of serializing just to do in-memory list queries.

## B+ Trees (bptree.ts)

B+ trees are a type of self-balancing tree used for storing sorted data to disk.

A B+ tree uses KeyValueDatabase for persistence and exposes an OrderedKeyValueDatabase api.

There are a bunch of interesting trade-offs to understand here...

1. You'll typically hear about red-black trees or AVL trees when it comes to binary trees. If you're storing data in-memory, these are really efficient approaches, but when it comes to persisting data to disk b+ trees are much better. That's because reading and writing to disk is an expensive operation. Operating systems typically have a page size around 4kb, for example, so it doesn't make sense to read just 16 bytes at a time from disk. Each node of B+ tree is typically optimized to be approximately an operating system page size. But since I'm building this in JavaScript, lets just go with 1000 keys per node. This means that you can you can reach a billion values in just 3 reads from disk — log_1000(1B) vs log_2(1B).

2. You don't often see durable hashmaps. There's obviously overhead of managing memory on disk and whatnot, but the bigger thing is that reading from disk usually the most expensive part. With B+ trees that are really wide, you end up with very few disk reads, and then searching through the keys in memory is much faster than a disk read so that's bacially trivial. Thus, if you are in search of a O(1) durable key-value store, you might be surprised that they're hard to find. Just use SQLite (b+ tree) or LevelDb (log-sorted merge tree).

The whole point of building a B+ tree is because I want to build an Interval tree to do reactivity efficiently.

## Immutable B+ Tree (bptree-tx.ts)

This version of a B+ tree makes sure that all mutations to the tree are immutable and uses a transaction encapsulation to accumulate reads and writes and commit them all at once.

Crucially, we added a `verifyImmutable` function to the tests to verify that we aren't mutably modifying anything. This sets us up well to use a proper key-value database for storage.

## B+ Tree with KeyValueDatabase (bptree-kv.ts)

Now we've officially swapped out for the KeyValueDatabase. This is a bit slower for the tests because of the read-consistency checks.

I'm not sure I like how concurrency stuff works here though. I can imagine a world in which writes / reads never make it through due to recurring conflicts. Seems like locks might be the way to go. The benefit of the DenoKV / FoundationDb approach with versionstamps is for coordinating writes in a distributed system. But for this B+Tree, its probably going to run in a single place / instance. So locks is an optimal approach in terms of throughput. Postgres uses locks afterall... but locks are annoying.

## B+ Tree with Locks (bptree-lock.ts)

[It seems](https://chat.openai.com/c/21ae6c66-fd90-400e-ab4c-14cf49f1f833) that Postgres uses page-level (aka node-level) read and write locks. Nodes are expanded on write, but interestingly, Postgres never merges / redistributes nodes on delete! That makes sense, honestly. It's an expensive operation, and for how much gain? How often are people deleting significant portions of an index without replacing with new values... Databases tend to grow over time! You need to manually call `REINDEX` if you want a clean tree.

Extended (`@rocicorp/lock`)[https://github.com/rocicorp/lock/pull/10] to handle a dynamic of locks — publishing under `@ccorcos/lock`. I experimented with a generator approach to using locks in `concurrency.ts`. It's cool, but I'm not sure its actually useful for what I need.

For b+ trees, there's an interesting approach called ["latch crabbing"](https://stackoverflow.com/questions/52058099/making-a-btree-concurrent-c) in which you release the locks of ancestor nodes once you know that there isn't going to be any merging or splitting of nodes above.

It's unclear how this approach would extend to Postgres or FoundationDb. I think for Postgres, we could potentially use plsql or plv8 to do all of this in the database -- that would be super cool. For foundationdb, I think we're stuck with retries. Worst case scenario, we'd have to pipe all writes through a single endpoint to prevent them from colliding. And at this point, we're just re-inventing locks. If it's a write-heavy index, then reads are going to retry constantly too.

Anyways, `kv-lock.ts` contains an async KeyValueDatabase with a RWLockMapo for managing locks. There's a risk of lock contention if two locks are waiting on each other forever.

A couple missing features for out B+ trees.
- [ ] list querying
- [ ] batch writes

## B+ Tree Improved (bptree2.ts)

I added better types with more semantically meaningful types for BranchNode and LeafNode, e.g. minKey for BranchNode instead of just key.

Keys can now have arbitrary types so long as you pass a comparator. That means you can use `jsonCodec.compare` to have tuples as keys.

I also added `list` query functionality.

The goal here is to get on our way to an interval tree...

## B+ Count Tree (bptree-count.ts)

The goal in some sense is to try to generalize this tree logic so that we can include an arbitrary aggregation function. To start, we're just going to hardcode a `count` which will keep track of how many elements are in a specific subtree. This will give us fast indexed aggregations.

Eventually, using GiST (generalized search tree) as an inspiration, we'll see if we can generalize from there.

## B+ Reducer Tree (bptree-reducer.ts)

This is a generalization of the count tree, using a reducer to essentially index aggregation queries.

The main downside to this approach is there's no chance for crab latching stuff with locks, so concurrency goes out the window. But maybe thats ok. I think that aggregations are fundamentally either eventually consistent or going to block on writes like this.

I was thinking I was going to make this into [GiST](https://gist.cs.berkeley.edu), but it's unclear how to do that. GiST only keeps track of keys and values. I'm not entirely sure it has a clear aggregation abstraction like this. I'm pretty happy with the API so far, so I'm going to stick with this for now.

## Interval B+ Tree (itree.ts)

There is a subtle difference between a [Segment Tree](ttps://www.dgp.toronto.edu/public_user/JamesStewart/378notes/22intervals/) and an [Interval Tree](https://en.wikipedia.org/wiki/Interval_tree). I find that in reality, they are often conflated with each other.

A Range Tree (aka rtree) is a 2d generalization of an interval tree. However typical rtree implementations are specifically designed for geospatial map data (e.g. `rbush`), thus the values of the ranges must be numerical. These numerical values are used to measure the size of a node and the centerpoint of a node to help determine which node to put a new element inside of. This is bad news for us because we want out keys to be lexicographical ranges!

SQLite's rtree index is 2D numerical. Postgres GiST index is complicated. For numerical values, it uses an rtree-like structure. But it does accept text values but then it does trigram matching which doesn't seem to be what we want.

This [CMU class 15-826](https://www.cs.cmu.edu/~christos/courses/826.F19/FOILS-pdf/130_SAMs_Rtrees.pdf), is a helpful resource for understadin R-trees though:

The most common usecase for an interval tree is for date range queries, e.g. "get me all of the events that overlap with this week". However, these dates are usually translated into a numerical value so that doesn't help us much.

The concept isn't all that complicated though... We have a tree sorted by start, and propagate the maxEnd for the node all the way up the tree.

## Performance Testing - Round 1

For 10k items, the bptree is just barely faster than an ordered array, but they're pretty comparable.

```sh
┌─────────┬──────────────┬────────────────────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                      │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼────────────────────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '004.559ms'  │ 'insert 10_000 ordered array'  │ '219'   │ '±0.36%' │ 439     │
│ 1       │ '004.037ms'  │ 'insert 10_000 bptree2 50-100' │ '247'   │ '±1.67%' │ 496     │
└─────────┴──────────────┴────────────────────────────────┴─────────┴──────────┴─────────┘
```

For 100k items, a bptree is almost 10x faster than an ordered arrray.

```sh
┌─────────┬──────────────┬──────────────────────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                        │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼──────────────────────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '401.652ms'  │ 'insert 100_000 ordered array'   │ '2'     │ '±3.47%' │ 5       │
│ 1       │ '070.908ms'  │ 'insert 100_000 bptree2 10-20'   │ '14'    │ '±2.89%' │ 29      │
│ 2       │ '052.286ms'  │ 'insert 100_000 bptree2 50-100'  │ '19'    │ '±2.06%' │ 39      │
│ 3       │ '051.514ms'  │ 'insert 100_000 bptree2 100-200' │ '19'    │ '±1.85%' │ 39      │
└─────────┴──────────────┴──────────────────────────────────┴─────────┴──────────┴─────────┘
```

With 100k items already, to insert 1000 more items, a btree is about 20x faster than an array.
With 100k items already, to delete 1000 items, a btree is about 3x slower.

```sh
┌─────────┬──────────────┬────────────────────────────────────────┬─────────┬───────────┬─────────┐
│ (index) │ Average Time │ Task Name                              │ ops/sec │ Margin    │ Samples │
├─────────┼──────────────┼────────────────────────────────────────┼─────────┼───────────┼─────────┤
│ 0       │ '012.094ms'  │ 'insert 1000 more from array 100k'     │ '82'    │ '±23.60%' │ 167     │
│ 1       │ '557.679μs'  │ 'insert 1000 more bptree2 50-100 100k' │ '1,793' │ '±0.52%'  │ 3587    │
│ 2       │ '169.889μs'  │ 'delete 1000 more from array 100k'     │ '5,886' │ '±0.22%'  │ 11773   │
│ 3       │ '547.311μs'  │ 'delete 1000 more bptree2 50-100 100k' │ '1,827' │ '±0.33%'  │ 3655    │
└─────────┴──────────────┴────────────────────────────────────────┴─────────┴───────────┴─────────┘
```

I want to do some more performance testing with SQLite and LevelDb for comparison. Will need to polish things up to get there.

## In-Memory B+ Tree (lib/InMemoryBinaryPlusTree.ts)

Starting from bptree2.ts
- Improved list() to allow open/closed bounds and reverse.
- Using a Map instead of an object to store nodes.


## Durable B+ Tree (lib/AsyncBinaryPlusTree.ts)

I'm not sure latch crabbing is really that useful once we start having batch writes and list reads. Probably better just to have a single read and write lock.

Starting from InMemoryBinaryPlusTree, made it async, added concurrency tests from `bptree-lock.test.ts`, and added immutability checks to make sure we aren't mutating anything.

## Storage

Implemented 4 kinds of storage for `AsyncKeyValueStorage`. This is interface is much simpler than tuple storage because keys are strings and it just stores the tree nodes.

- JsonFileKeyValueStorage
- LevelDbKeyValueStorage
- SQLiteKeyValueStorage
- IndexedDbKeyValueStorage

Wrote some simple property tests just to verify they work.

## Performance Testing - Round 2


First, lets compare tree size performance. There's a trade-off between how big pages are and how costly a disk read is. However, it's totally possible (and likely) that the disk pages are cached in memory anyways since its likely only a MB or two or data.

```sh
┌─────────┬──────────────┬───────────────────────┬─────────┬───────────┬─────────┐
│ (index) │ Average Time │ Task Name             │ ops/sec │ Margin    │ Samples │
├─────────┼──────────────┼───────────────────────┼─────────┼───────────┼─────────┤
│ 0       │ '504.696ms'  │ 'b+level 50-100'      │ '1'     │ '±5.88%'  │ 4       │
│ 1       │ '474.645ms'  │ 'b+level 100-200'     │ '2'     │ '±3.55%'  │ 5       │
│ 2       │ '579.890ms'  │ 'b+level 200-400'     │ '1'     │ '±3.01%'  │ 4       │
│ 3       │ '828.546ms'  │ 'b+level 400-800'     │ '1'     │ '±4.55%'  │ 3       │
│ 4       │ '001.358s'   │ 'b+level 800-1600'    │ '0'     │ '±11.91%' │ 2       │
│ 5       │ '002.426s'   │ 'b+level 2000-4000'   │ '0'     │ '±2.71%'  │ 2       │
│ 6       │ '009.078s'   │ 'b+level 10000-20000' │ '0'     │ '±3.50%'  │ 2       │
└─────────┴──────────────┴───────────────────────┴─────────┴───────────┴─────────┘
```

Considering that Postgres doesn't even merge/redistribute on deletes, it's worth trying an approach that has a really small minSize. That will add some cost for subsequent reads after deletes but maybe it's not too bad.

```sh
┌─────────┬──────────────┬───────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name         │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼───────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '564.125ms'  │ 'b+level 4-9'     │ '1'     │ '±9.49%' │ 4       │
│ 1       │ '440.392ms'  │ 'b+level 10-20'   │ '2'     │ '±4.44%' │ 5       │
│ 2       │ '370.472ms'  │ 'b+level 20-40'   │ '2'     │ '±1.96%' │ 6       │
│ 3       │ '463.307ms'  │ 'b+level 50-100'  │ '2'     │ '±2.68%' │ 5       │
│ 4       │ '462.864ms'  │ 'b+level 1-100'   │ '2'     │ '±2.80%' │ 5       │
│ 5       │ '467.674ms'  │ 'b+level 10-100'  │ '2'     │ '±4.14%' │ 5       │
│ 6       │ '466.507ms'  │ 'b+level 20-100'  │ '2'     │ '±2.35%' │ 5       │
│ 7       │ '474.350ms'  │ 'b+level 100-200' │ '2'     │ '±2.83%' │ 5       │
│ 8       │ '491.414ms'  │ 'b+level 1-200'   │ '2'     │ '±2.57%' │ 5       │
│ 9       │ '485.670ms'  │ 'b+level 10-200'  │ '2'     │ '±2.34%' │ 5       │
│ 10      │ '490.234ms'  │ 'b+level 40-200'  │ '2'     │ '±6.45%' │ 5       │
│ 11      │ '602.829ms'  │ 'b+level 200-400' │ '1'     │ '±4.48%' │ 4       │
│ 12      │ '588.624ms'  │ 'b+level 10-400'  │ '1'     │ '±2.17%' │ 4       │
│ 13      │ '837.142ms'  │ 'b+level 400-800' │ '1'     │ '±4.55%' │ 3       │
└─────────┴──────────────┴───────────────────┴─────────┴──────────┴─────────┘
```

Seems like 1-40 is a the best. We'll use that size going forward.

```sh
┌─────────┬──────────────┬─────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name       │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼─────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '552.480ms'  │ 'b+level 4-9'   │ '1'     │ '±4.59%' │ 4       │
│ 1       │ '454.746ms'  │ 'b+level 10-20' │ '2'     │ '±3.92%' │ 5       │
│ 2       │ '439.856ms'  │ 'b+level 1-20'  │ '2'     │ '±3.65%' │ 5       │
│ 3       │ '395.749ms'  │ 'b+level 20-40' │ '2'     │ '±3.90%' │ 6       │
│ 4       │ '386.357ms'  │ 'b+level 1-40'  │ '2'     │ '±4.61%' │ 6       │
│ 5       │ '425.084ms'  │ 'b+level 40-80' │ '2'     │ '±2.38%' │ 5       │
│ 6       │ '435.143ms'  │ 'b+level 1-80'  │ '2'     │ '±5.92%' │ 5       │
└─────────┴──────────────┴─────────────────┴─────────┴──────────┴─────────┘
```

Now obviously using this b+tree is going to be slower than using SQLite or LevelDb directly. But the benefit of the b+ tree comes down the line from the reducer tree and the interval tree. The goal here is just to get some sense of it's performance and the relative trade-off.

Looks like the b+ tree is 1/3x slower for SQLite and 5x slower for LevelDb for consecutive writes. However for batch writes, the performance degration is more marginal.

Very relevant though is that SQlite is about 20x slower than LevelDb.

```sh
┌─────────┬──────────────┬────────────────────────────────┬─────────┬───────────┬─────────┐
│ (index) │ Average Time │ Task Name                      │ ops/sec │ Margin    │ Samples │
├─────────┼──────────────┼────────────────────────────────┼─────────┼───────────┼─────────┤
│ 0       │ '004.485ms'  │ 'insert 10_000 memory'         │ '222'   │ '±0.80%'  │ 446     │
│ 1       │ '002.906s'   │ 'insert 10_000 sqlite'         │ '0'     │ '±19.72%' │ 2       │
│ 2       │ '003.843s'   │ 'insert 10_000 b+sqlite'       │ '0'     │ '±1.70%'  │ 2       │
│ 3       │ '119.468ms'  │ 'insert 10_000 level'          │ '8'     │ '±2.00%'  │ 17      │
│ 4       │ '614.924ms'  │ 'insert 10_000 b+level'        │ '1'     │ '±5.15%'  │ 4       │
│ 5       │ '015.546ms'  │ 'insert batch 10_000 sqlite'   │ '64'    │ '±0.45%'  │ 129     │
│ 6       │ '017.458ms'  │ 'insert batch 10_000 b+sqlite' │ '57'    │ '±1.36%'  │ 115     │
│ 7       │ '013.497ms'  │ 'insert batch 10_000 level'    │ '74'    │ '±0.36%'  │ 149     │
│ 8       │ '017.038ms'  │ 'insert batch 10_000 b+level'  │ '58'    │ '±2.31%'  │ 118     │
└─────────┴──────────────┴────────────────────────────────┴─────────┴───────────┴─────────┘
```

Since we're usually dealing with a database already with some size, lets measure performance on a tree with 100k items already it.

B+ consecutive deletes are 40x slower with SQLite, 8x slower with LevelDb. Reads are about 8x slower for both SQLite and LevelDb.

```sh
┌─────────┬──────────────┬────────────────────────────────────────┬─────────┬───────────┬─────────┐
│ (index) │ Average Time │ Task Name                              │ ops/sec │ Margin    │ Samples │
├─────────┼──────────────┼────────────────────────────────────────┼─────────┼───────────┼─────────┤
│ 0       │ '324.898ms'  │ 'insert 1000 more from 100k sqlite'    │ '3'     │ '±10.80%' │ 7       │
│ 1       │ '408.879ms'  │ 'insert 1000 more from 100k b+ sqlite' │ '2'     │ '±1.85%'  │ 5       │
│ 2       │ '013.178ms'  │ 'insert 1000 more from 100k level'     │ '75'    │ '±2.74%'  │ 153     │
│ 3       │ '091.072ms'  │ 'insert 1000 more from 100k b+ level'  │ '10'    │ '±5.32%'  │ 22      │
│ 4       │ '011.128ms'  │ 'delete 1000 more from 100k sqlite'    │ '89'    │ '±27.60%' │ 180     │
│ 5       │ '403.024ms'  │ 'delete 1000 more from 100k b+ sqlite' │ '2'     │ '±4.86%'  │ 5       │
│ 6       │ '011.059ms'  │ 'delete 1000 more from 100k level'     │ '90'    │ '±1.86%'  │ 182     │
│ 7       │ '084.786ms'  │ 'delete 1000 more from 100k b+ level'  │ '11'    │ '±2.86%'  │ 24      │
│ 8       │ '007.443ms'  │ 'read 1000 from 100k sqlite'           │ '134'   │ '±0.74%'  │ 269     │
│ 9       │ '052.751ms'  │ 'read 1000 from 100k b+ sqlite'        │ '18'    │ '±1.10%'  │ 38      │
│ 10      │ '007.628ms'  │ 'read 1000 from 100k level'            │ '131'   │ '±0.68%'  │ 263     │
│ 11      │ '055.003ms'  │ 'read 1000 from 100k b+ level'         │ '18'    │ '±0.60%'  │ 37      │
└─────────┴──────────────┴────────────────────────────────────────┴─────────┴───────────┴─────────┘
```

Conclusions:
- LevelDb is way faster at writes and SQLite. Deletes and reads are about the same.
- B+ tree layer adds 1/2x (sqlite) - 8x (level) cost on consecutive writes.
- B+ tree layer adds 8x (level) - 40x (sqlite) cost on consecutive deletes.
- B+ tree layer ads about 8x cost on reads.

If we stick with LevelDb, we can safely say "its about 8x slower".

Idea: something to test for performance; are there any cases where we're writing a node that hasn't actually changed?

## Durable B+ Reducer Tree

- `InMemoryReducerTree.ts`
- `AsyncReducerTree.ts`

## Durable B+ Interval Tree

- `InMemoryIntervalTree.ts`
- `AsyncIntervalTree.ts`

## Performance Testing - Round 3

After putting 2000 numbers in a table, compute the sum of various ranges across those numbers.

SQLite is O(n) and b+ tree is O(log n).

```sh
┌─────────┬──────────────┬────────────────────────────┬──────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                  │ ops/sec  │ Margin   │ Samples │
├─────────┼──────────────┼────────────────────────────┼──────────┼──────────┼─────────┤
│ 0       │ '099.692μs'  │ 'b+reducer on leveldb sum' │ '10,030' │ '±0.35%' │ 20062   │
│ 1       │ '254.870μs'  │ 'sqlite sum'               │ '3,923'  │ '±0.38%' │ 7848    │
└─────────┴──────────────┴────────────────────────────┴──────────┴──────────┴─────────┘
```

Now let's try the interval tree. For 20,000 completely random ranges in the database, then overlaps is faster on SQLite because we aren't able to effectively trim nodes from the interval tree.

Since the ranges are random, then odds are that in every leaf node is a range that spans the rest of the tree! This means we'll get the same big-O performance of the SQLite btree index!

```sh
┌─────────┬──────────────┬──────────────────────────────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                                │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼──────────────────────────────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '026.762ms'  │ 'random ranges interval tree on leveldb' │ '37'    │ '±1.99%' │ 75      │
│ 1       │ '006.063ms'  │ 'random ranges sqlite overlaps'          │ '164'   │ '±0.41%' │ 330     │
└─────────┴──────────────┴──────────────────────────────────────────┴─────────┴──────────┴─────────┘
```

Imagine a calendar, though, with all events having a random start and end date. That's not a realistic distribution.

So let's imagine smaller ranges. Rather than randomly sample from the entire range, we'll sample a "duration" that's only 5% of the range.

In the worst case, we'll still have to fetch several leaf nodes though. For example, we're using a tree width of 40, and 40/20000 = 0.002. So we're going to have to fetch 0.05/0.002 = 25 leaf nodes just to find overlaps with a single point.

```sh
┌─────────┬──────────────┬────────────────────────────────────────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                                          │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼────────────────────────────────────────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '003.196ms'  │ '20000 items, 5% ranges, interval tree on leveldb' │ '312'   │ '±0.57%' │ 626     │
│ 1       │ '001.324ms'  │ '20000 items, 5% ranges, sqlite overlaps'          │ '755'   │ '±1.54%' │ 1511    │
└─────────┴──────────────┴────────────────────────────────────────────────────┴─────────┴──────────┴─────────┘
```

Dropping the size of the range leads to better perf over time compared to the SQLite scan.

```sh
┌─────────┬──────────────┬─────────────────────────────────────────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                                           │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼─────────────────────────────────────────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '004.800ms'  │ '100000 items, 2% ranges, interval tree on leveldb' │ '208'   │ '±0.71%' │ 417     │
│ 1       │ '006.708ms'  │ '100000 items, 2% ranges, sqlite overlaps'          │ '149'   │ '±0.27%' │ 299     │
└─────────┴──────────────┴─────────────────────────────────────────────────────┴─────────┴──────────┴─────────┘
```

Let's consider a more realistic calendar view...

2 decades of data:
- 5-15x 15min-3hr long meetings / week
- 3-12x 1day-4day events per month.
- 2-5x 5day-20day events per year.

Then we'll query for every day view, every week view, and every month view.

Approximately 10k events and 8784 queries.

```sh
┌─────────┬──────────────┬─────────────────────────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                           │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼─────────────────────────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '656.892ms'  │ 'calendar interval tree on leveldb' │ '1'     │ '±1.69%' │ 20      │
│ 1       │ '001.752s'   │ 'calendar sqlite overlaps'          │ '0'     │ '±0.68%' │ 20      │
└─────────┴──────────────┴─────────────────────────────────────┴─────────┴──────────┴─────────┘
```

Lets try 20 decades to simulate having many more users.
100k events, 87624 ranges. But we're going to sample 1000 ranges at a time.

Ok, now we're ~20x faster than SQLite.

```sh
┌─────────┬──────────────┬─────────────────────────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                           │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼─────────────────────────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '101.121ms'  │ 'calendar interval tree on leveldb' │ '9'     │ '±2.19%' │ 20      │
│ 1       │ '002.144s'   │ 'calendar sqlite overlaps'          │ '0'     │ '±1.48%' │ 10      │
└─────────┴──────────────┴─────────────────────────────────────┴─────────┴──────────┴─────────┘
```

Lets do the same thing but add a "parasitic event" that spans the entire range duration. This will force SQlite to do a O(n) full table scan, but the interval tree in the best case is just O(2*log n).

```sh
┌─────────┬──────────────┬───────────────────────────────────────────────┬─────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                                     │ ops/sec │ Margin   │ Samples │
├─────────┼──────────────┼───────────────────────────────────────────────┼─────────┼──────────┼─────────┤
│ 0       │ '164.021ms'  │ 'parasitic calendar interval tree on leveldb' │ '6'     │ '±2.18%' │ 13      │
│ 1       │ '002.334s'   │ 'parasitic calendar sqlite overlaps'          │ '0'     │ '±1.60%' │ 10      │
└─────────┴──────────────┴───────────────────────────────────────────────┴─────────┴──────────┴─────────┘
```

Let's [compile SQLite](https://github.com/WiseLibs/better-sqlite3/blob/60763a0742690c8bae1db43d838f418cfc83b656/docs/compilation.md) with the [R*Tree extension](https://www.sqlite.org/rtree.html) just for another point of comparison.

```sh
npm uninstall better-sqlite3
sh build-sqlite.sh
```

Here we go... way faster! And I think that's expected. The btree is using JavaScript and needs to do a bunch of serialization. But at least we have some idea of how much improvement we can get from using a different language: ~30x.

The good news is that the in-memory tree is still wicked fast – faster than SQLite in-memory. Probably because SQLite has the serialization cost in this case.

```sh
┌─────────┬──────────────┬──────────────────────────────────────────────────────┬──────────┬──────────┬─────────┐
│ (index) │ Average Time │ Task Name                                            │ ops/sec  │ Margin   │ Samples │
├─────────┼──────────────┼──────────────────────────────────────────────────────┼──────────┼──────────┼─────────┤
│ 0       │ '015.617μs'  │ 'parasitic calendar interval tree in memory'         │ '64,032' │ '±0.50%' │ 128066  │
│ 1       │ '169.280ms'  │ 'parasitic calendar interval tree on leveldb'        │ '5'      │ '±1.66%' │ 12      │
│ 2       │ '006.252ms'  │ 'parasitic calendar sqlite rtree overlaps'           │ '159'    │ '±0.31%' │ 320     │
│ 3       │ '001.317ms'  │ 'parasitic calendar sqlite rtree overlaps in memory' │ '759'    │ '±0.23%' │ 1519    │
└─────────┴──────────────┴──────────────────────────────────────────────────────┴──────────┴──────────┴─────────┘
```

# What's next?

- RW lock should be on at the storage layer.
- Transaction should be at the storage layer.
- Tree abstractions are stateless.

- The database client uses trees as it pleases, ties together concurrency and reactivity.
- Query generator transaction abstraction.

- version updates and stuff is application-layer.








Game plan...
- first create some examples with hardcoded indexing logic
- then create some kind of syntax for persisting indexes
- then implement all the shared transaction stuff.

examples
- messaging app
- social app
- calendar app
- notes app
- end-user database app



-> examples/messaging.ts


plan for database-experiments...
- fit everything into an api so we can do perf comparisons easier
- how to mix and match trees transactionally?

- how to mix interval trees with btrees? Suppose I want an interval tree per user... Will this work? [[userId, start], [userId, end]]
	- perf comparison for this kind of structure.




thinking... postgres, denokv, foundationdb for storage?


plan for tuple-database...
- use lexicodec
- remove types
- use generators for transactions
- use interval tree for subscriptions
- use versionstamp for concurrency?
examples
- messaging app
- calendar app
- notes app
- end-user database app




- an object model for usability.
- serializable schemas for end-user UI
- dev ui







Generator yield types coming soon!
https://github.com/microsoft/TypeScript/issues/36967
-> https://github.com/microsoft/TypeScript/issues/43632
-> https://www.matechs.com/blog/abusing-typescript-generators


Demos...
- Start over with a contacts app. Schema and UI. Introduce users, auth, and permission later.
- Messaging app (Slack)
- Social network app (Twitter)
- Contacts app  (Database)
- Generalized Database (Airtable)
- Filing Cabinets
