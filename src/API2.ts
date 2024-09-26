/************************************************************************************

Trade-offs...
- Optional consistency checks makes sense for a larger scale deployment. And makes sense for
	certain types of databases, allowing you to create transactions in javascript.
- Baked in consistency is less flexible, but also means you don't need to think about it.
- IndexedDb has its own transaciton capabilities.
- LevelDb does not and you probably have to just serialize writes.
- Postgres and FoundationDb would benefit from explicit checks, though you can do that under the hood for everything.

*************************************************************************************/

/************************************************************************************

SYNC API

*************************************************************************************/

export type KeyValueApi<K = (string | number)[], V = any> = {
	get: (key: K) => V | undefined
	write: (writes: { set?: { key: K; value: V }[]; delete?: K[] }) => void
}

export type OrderedKeyValueApi<K = (string | number)[], V = any> = KeyValueApi<K, V> & {
	list: (args?: { gt?: K; gte?: K; lt?: K; lte?: K; limit?: number; reverse?: boolean }) => { key: K; value: V }[]
}

export type IntervalTreeApi<B = (string | number)[], K = (string | number)[], V = any> = KeyValueApi<[B, B, K], V> & {
	overlaps: (args?: {
		gt?: B
		gte?: B
		lt?: B
		lte?: B
		limit?: number
		reverse?: boolean
	}) => Promise<{ key: [B, B, K]; value: V }[]>
}

/************************************************************************************

ASYNC API

I'm very frustrated. My head is cloudy... Need another reset, unfortunately.
Where is this headed?

- How can I unify explicit consistency checks with implicit locks.
- How can I unify async and sync queries? (or maybe I don't).

*************************************************************************************/

export type AsyncKeyValueApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>
	write: (writes: { set?: { key: K; value: V }[]; delete?: K[]; check?: { key: K; value: V }[] }) => Promise<void>
}

export type AsyncOrderedKeyValueApi<K = string, V = any> = AsyncKeyValueApi<K, V> & {
	list: (args?: {
		gt?: K
		gte?: K
		lt?: K
		lte?: K
		limit?: number
		reverse?: boolean
	}) => Promise<{ key: K; value: V }[]>
}

export type AsyncIntervalTreeApi<B = string, K = string, V = any> = AsyncKeyValueApi<[B, B, K], V> & {
	overlaps: (args?: {
		gt?: B
		gte?: B
		lt?: B
		lte?: B
		limit?: number
		reverse?: boolean
	}) => Promise<{ key: [B, B, K]; value: V }[]>
}

type WriteArgs<K = string, V = any> = {
	set?: { key: K; value: V }[]
	delete?: K[]
	check?: { key: K; value: V }[]
}

import * as t from "./examples/dataTypes"

// A social network.
const UserSchema = t.object({ id: t.uuid, username: t.string })
const FollowSchema = t.object({ from: t.uuid, to: t.uuid, created: t.datetime })
const PostSchema = t.object({
	id: t.uuid,
	created: t.datetime,
	author: t.uuid,
	content: t.string,
	// Imagine this is also a calendar event.
	time: t.tuple(t.datetime, t.datetime),
})

const Schema = [
	{ key: ["user", t.uuid], value: UserSchema },
	{ key: ["usernames", t.string, t.uuid], value: null },
	{ key: ["follows", t.uuid, t.uuid], value: FollowSchema },
	{ key: ["followers", t.uuid, t.uuid], value: null },
	{ key: ["post", t.uuid], value: PostSchema },
	/* user.id, created, post.id  */
	{ key: ["profile", t.uuid, t.datetime, t.uuid], value: null },
	/* user.id, created, post.id  */
	{ key: ["timeline", t.uuid, t.datetime, t.uuid], value: null },
	/* user.id, start, end, post.id  */
	{ key: ["calendar", t.uuid, t.datetime, t.datetime, t.uuid], value: null },

	{ key: ["version", t.any], value: t.number },
]

/************************************************************************************

- How can we compose different kinds of trees easier?
- How can we handle versions and consistency checks better.

- need to be able to index across tables
- need to be able to use a mixture of index types (interval tree vs binary+ tree).
- need a unified sync/async query process.

- consistency checks
- interval tree
- tedious index definitions
- async vs sync.

*************************************************************************************/

// https://github.com/vlcn-io/materialite
const db: OrderedKeyValueApi<any, any> = {} as any
type DB = typeof db
type TX = Transaction<any, any>

// User
// Follow
// Post
// Profile
// Timeline

const users = Collection(["user"], [t.uuid])

const set = (tx, key, value) => {}
const inc = (tx, key) => {}

const createUser = (db: DB, tx: TX, user: t.Infer<typeof UserSchema>) => {
	set(tx, ["user", user.id], user)
	inc(tx, ["version", "user", user.id])

	set(tx, ["usernames", user.username, user.id], null)
}

// function check(tx: any, key: any) {
// 	const version = tx.get(["version", key])
// 	tx.check(["version", key], version)
// }

// function inc(tx: any, key: any) {
// 	tx.inc(["version", key])
// }

// const indexPost = (tx: any, userId: string, post: any) => {
// 	tx.set(["timeline", userId, post.created, post.id])
// 	inc(tx, ["timeline", userId])

// 	tx.set(["calendar", userId, post.start, post.end, post.id], null)
// 	inc(tx, ["calendar", userId])
// }

// const createFollow = (tx: any, follow: any) => {
// 	tx.set(["follows", follow.from, follow.to], follow)
// 	inc(tx, ["follows", follow.from])

// 	tx.set(["followers", follow.to, follow.from], null)
// 	inc(tx, ["followers", follow.to])

// 	// Add posts to timeline.
// 	check(tx, ["profile", follow.to])
// 	for (const { key } of tx.list({ prefix: ["profile", follow.to] })) {
// 		const [_1, _2, _3, postId] = key

// 		check(tx, ["post", postId])
// 		const post = tx.get(["post", postId])

// 		indexPost(tx, follow.from, post)
// 	}
// }

// const createPost = (tx: any, post: any) => {
// 	tx.set(["post", post.id], post)
// 	inc(tx, ["post", post.id])

// 	tx.set(["profile", post.author, post.created, post.id], null)
// 	inc(tx, ["profile", post.author])

// 	// Add post to timeline
// 	check(tx, ["followers", post.author])
// 	for (const { key } of tx.list({ prefix: ["followers", post.author] })) {
// 		const [_1, _2, userId] = key
// 		indexPost(tx, userId, post)
// 	}
// }

// const unindexPost = (tx: any, userId: string, post: any) => {
// 	tx.delete(["timeline", userId, post.created, post.id])
// 	inc(tx, ["timeline", userId])

// 	tx.delete(["calendar", userId, post.start, post.end, post.id], null)
// 	inc(tx, ["calendar", userId])
// }

// const deleteFollow = (tx: any, from: string, to: string) => {
// 	check(tx, ["follows", from, to])
// 	const follow = tx.get(["follows", from, to])

// 	tx.delete(["follows", follow.from, follow.to])
// 	inc(tx, ["follows", follow.from])

// 	tx.delete(["followers", follow.to, follow.from])
// 	inc(tx, ["followers", follow.to])

// 	// Remove posts from timeline.
// 	check(tx, ["profile", follow.to])
// 	for (const { key } of tx.list({ prefix: ["profile", follow.to] })) {
// 		const [_1, _2, _3, postId] = key

// 		check(tx, ["post", postId])
// 		const post = tx.get(["post", postId])
// 		unindexPost(tx, follow.from, post)
// 	}
// }

// const deletePost = (tx: any, postId: any) => {
// 	check(tx, ["post", postId])
// 	const post = tx.get(["post", postId])

// 	tx.delete(["post", post.id], post)
// 	inc(tx, ["post", postId])

// 	tx.delete(["profile", post.author, post.created, post.id], null)
// 	inc(tx, ["profile", post.author])

// 	// Add post to timeline
// 	check(tx, ["followers", post.author])
// 	for (const { key } of tx.list({ prefix: ["followers", post.author] })) {
// 		const [_1, _2, userId] = key
// 		unindexPost(tx, userId, post)
// 	}
// }

// const deleteUser = (tx: any, userId: string) => {
// 	check(tx, ["user", userId])
// 	const user = tx.get(["user", userId])

// 	tx.delete(["user", user.id], user)
// 	inc(tx, ["user", user.id])

// 	tx.delete(["usernames", user.username, user.id], null)

// 	check(tx, ["follows", userId])
// 	for (const { key } of tx.list({ prefix: ["follows", userId] })) {
// 		const [_1, from, to] = key
// 		deleteFollow(tx, from, to)
// 	}

// 	check(tx, ["followers", userId])
// 	for (const { key } of tx.list({ prefix: ["followers", userId] })) {
// 		const [_1, to, from] = key
// 		deleteFollow(tx, from, to)
// 	}

// 	check(tx, ["profile", userId])
// 	for (const { key } of tx.list({ prefix: ["profile", userId] })) {
// 		const [_1, _2, _3, postId] = key
// 		deletePost(tx, postId)
// 	}
// }
