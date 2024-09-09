/************************************************************************************

Lets think through a minimal API and go from there.

*************************************************************************************/

export type KeyValueApi<K = string, V = any> = {
	get: (key: K) => V | undefined
	write: (writes: { set?: { key: K; value: V }[]; delete?: K[]; check?: { key: K; value: V }[] }) => void
}

export type OrderedKeyValueApi<K = string, V = any> = KeyValueApi<K, V> & {
	list: (args?: { gt?: K; gte?: K; lt?: K; lte?: K; limit?: number; reverse?: boolean }) => { key: K; value: V }[]
}

export type IntervalTreeApi<B, K, V> = KeyValueApi<[B, B, K], V> & {
	overlaps: (args?: {
		gt?: B
		gte?: B
		lt?: B
		lte?: B
		limit?: number
		reverse?: boolean
	}) => Promise<{ key: [B, B, K]; value: V }[]>
}

export type AsyncKeyValueApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>
	write: (writes: { set?: { key: K; value: V }[]; delete?: K[]; check?: { key: K; value: V }[] }) => Promise<void>
}

export type AsyncKeyValueTxApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>
	set: (key: K, value: V) => undefined
	delete: (key: K) => undefined
	check: (key: K, value: V) => undefined
	commit: () => Promise<undefined>
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

export type AsyncOrderedKeyValueTxApi<K = string, V = any> = AsyncOrderedKeyValueApi<K, V> & {
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

export type AsyncIntervalTreeTxApi<B = string, K = string, V = any> = AsyncKeyValueApi<[B, B, K], V> & {
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

Now, lets figure out how to use these abstractions together with some examples.

- need to be able to index across tables
- need to be able to use a mixture of index types (interval tree vs binary+ tree).
- need a unified sync/async query process.

*************************************************************************************/

import * as t from "./examples/dataTypes"

// A social network.
// const Schema = [
// 	{ key: ["user", t.uuid], value: t.object({ id: t.uuid, username: t.string }) },
// 	{ key: ["usernames", t.string, t.uuid], value: null },
// 	{ key: ["follows", t.uuid, t.uuid], value: { from: t.uuid, to: t.uuid, created: t.datetime } },
// 	{ key: ["followers", t.uuid, t.uuid], value: null },
// 	{ key: ["post", t.uuid], value: t.object({ id: t.uuid, created: t.datetime, author: t.uuid, content: t.string }) },
// 	/* user.id, created, post.id  */
// 	{ key: ["profile", t.uuid, t.datetime, t.uuid], value: null },
// 	/* user.id, created, post.id  */
// 	{ key: ["profile", t.uuid, t.datetime, t.uuid], value: null },
// ]

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
]

// const users = db.btree<[string], User>("users")
// const usernames = db.btree<[string, string], null>("usernames")

// db.itree()
// db.kv()

// TODO:
// - consistency checks
// - interval tree
// - tedious index definitions
// - async vs sync.

const createUser = (tx: any, user: any) => {
	tx.set(["user", user.id], user)
	tx.set(["usernames", user.username, user.id], null)
}

const indexPost = (tx: any, userId: string, post: any) => {
	tx.set(["timeline", userId, post.created, post.id])
	tx.set(["calendar", userId, post.start, post.end, post.id], null)
}

const createFollow = (tx: any, follow: any) => {
	tx.set(["follows", follow.from, follow.to], follow)
	tx.set(["followers", follow.to, follow.from], null)

	// Add posts to timeline.
	for (const { key } of tx.list({ prefix: ["profile", follow.to] })) {
		const [_1, _2, _3, postId] = key
		const post = tx.get(["post", postId])
		indexPost(tx, follow.from, post)
	}
}

const createPost = (tx: any, post: any) => {
	tx.set(["post", post.id], post)
	tx.set(["profile", post.author, post.created, post.id], null)

	// Add post to timeline
	for (const { key } of tx.list({ prefix: ["followers", post.author] })) {
		const [_1, _2, userId] = key
		indexPost(tx, userId, post)
	}
}

const unindexPost = (tx: any, userId: string, post: any) => {
	tx.delete(["timeline", userId, post.created, post.id])
	tx.delete(["calendar", userId, post.start, post.end, post.id], null)
}

const deleteFollow = (tx: any, from: string, to: string) => {
	const follow = tx.get(["follows", from, to])

	tx.delete(["follows", follow.from, follow.to])
	tx.delete(["followers", follow.to, follow.from])

	// Remove posts from timeline.
	for (const { key } of tx.list({ prefix: ["profile", follow.to] })) {
		const [_1, _2, _3, postId] = key
		const post = tx.get(["post", postId])
		unindexPost(tx, follow.from, post)
	}
}

const deletePost = (tx: any, postId: any) => {
	const post = tx.get(["post", postId])

	tx.delete(["post", post.id], post)
	tx.delete(["profile", post.author, post.created, post.id], null)

	// Add post to timeline
	for (const { key } of tx.list({ prefix: ["followers", post.author] })) {
		const [_1, _2, userId] = key
		unindexPost(tx, userId, post)
	}
}

const deleteUser = (tx: any, userId: string) => {
	const user = tx.get(["user", userId])
	tx.delete(["user", user.id], user)
	tx.delete(["usernames", user.username, user.id], null)

	for (const { key } of tx.list({ prefix: ["follows", userId] })) {
		const [_1, from, to] = key
		deleteFollow(tx, from, to)
	}

	for (const { key } of tx.list({ prefix: ["followers", userId] })) {
		const [_1, to, from] = key
		deleteFollow(tx, from, to)
	}

	for (const { key } of tx.list({ prefix: ["profile", userId] })) {
		const [_1, _2, _3, postId] = key
		deletePost(tx, postId)
	}
}