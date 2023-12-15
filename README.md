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

## Interval Trees (TODO)


- Interval tree for efficient reactivity.
- ChetStack with versionstamps?
