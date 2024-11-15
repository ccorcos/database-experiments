import { strict as assert } from "assert"
import * as t from "data-type-ts"
import { jsonCodec } from "lexicodec"
import { BTreeDb } from "../tupledb/BTreeDb"

// TODO:
// - follower timeline, you can't index with sql.
// - async database with yields.

/** All records must be objects with an `id: t.string` */
const tables = {
	user: t.object({
		id: t.string,
		name: t.string,
	}),
	channel: t.object({
		id: t.string,
		name: t.string,
	}),
	message: t.object({
		id: t.string,
		text: t.string,
		created_at: t.string,
		channel_id: t.string,
		author_id: t.string,
		draft: t.optional(t.boolean),
	}),
	follow: t.object({
		id: t.string,
		user_id: t.string,
		channel_id: t.string,
	}),
}

type Schema = { [K in keyof typeof tables]: t.Infer<(typeof tables)[K]> }

type AndFilter = { and: Filter[] }
type OrFilter = { or: Filter[] }
type PropertyFilter = {
	[variable: string]:
		| ExistenceFilter
		| StringFilter
		| BooleanFilter
		| NumberFilter
}

type ExistenceFilter = { null: boolean }
type BooleanFilter = { eq: boolean }
type StringFilter =
	| { eq: string }
	| { neq: string }
	| { contains: string }
	| { ncontains: string }
	| { startsWith: string }
	| { endsWith: string }
type NumberFilter =
	| { eq: number }
	| { neq: number }
	| { gt: number }
	| { gte: number }
	| { lt: number }
	| { lte: number }

type Filter = AndFilter | OrFilter | PropertyFilter

type Index = {
	select: {
		[K in keyof Schema]?: {
			[P in keyof Schema[K]]?: string
		}
	}[]
	filter?: Filter
	sort: string[]
}

const indexes: { [V: string]: Index } = {
	usersByName: {
		select: [{ user: { name: "name", id: "id" } }],
		sort: ["name", "id"],
	},

	channelMessages: {
		select: [
			{ message: { id: "message", channel_id: "channel", created_at: "time" } },
		],
		sort: ["channel", "time", "message"],
	},

	userFollowing: {
		select: [{ follow: { channel_id: "channel", user_id: "user" } }],
		sort: ["user", "channel"],
	},

	channelFollowers: {
		select: [{ follow: { id: "id", user_id: "user", channel_id: "channel" } }],
		sort: ["channel", "user"],
	},

	// select * from message
	// join follow on message.channel_id = follow.channel_id
	// where message.draft = false
	// order by follow.user_id, message.created_at, message.id

	timeline: {
		select: [
			{ follow: { user_id: "user", channel_id: "channel" } },
			{
				message: {
					id: "message",
					channel_id: "channel",
					created_at: "time",
					draft: "draft",
				},
			},
		],
		filter: { or: [{ draft: { null: true } }, { draft: { eq: false } }] },
		sort: ["user", "time", "message"],
	},

	// SELECT a.*, b.*
	// FROM follow AS a
	// JOIN follow AS b ON a.channel_id = b.channel_id
	// WHERE a.user_id != b.user_id
	// ORDER BY a.user_id, b.user_id;

	cofollowers: {
		select: [
			{ follow: { user_id: "userA", channel_id: "channel" } },
			{ follow: { user_id: "userB", channel_id: "channel" } },
		],
		filter: { userA: { neq: "userB" } },
		sort: ["userA", "userB"],
	},

	// cofollowers2: {
	// 	select: { a: "follow", b: "follow" },
	// 	filter: {
	// 		and: [
	// 			{ "a.channel_id": { eq: "b.channel_id" } },
	// 			{ "a.user_id": { neq: "b.user_id" } },
	// 		],
	// 	},
	// 	sort: ["a.user_id", "b.user_id"],
	// },
}

// STOP

const db = new BTreeDb()

function insert<T extends keyof Schema>(table: T, value: Schema[T]) {
	const error = tables[table].validate(value)
	if (error) throw new Error(t.formatError(error))

	const prev = db.get([table, value.id])

	if (prev) {
		for (const [indexName, index] of Object.entries(indexes)) {
			const vars = {}
			for (const select of index.select) {
				const varMap = select[table]
				if (!varMap) continue
				for (const [colName, varName] of Object.entries(varMap)) {
					vars[varName] = prev[colName]
				}
			}

			const tuple = index.sort.map((varName) => {
				if (!(varName in vars)) throw new Error("Missing ")
			})
		}
	}

	// db.set([table, "byId", value.id], value)
	// const tableIndexes = indexes[table]
	// if (tableIndexes) {
	// 	for (const [index, columns] of Object.entries(tableIndexes)) {
	// 		if (index === "byId") continue
	// 		const keys = columns.map((col) => value[col])
	// 		db.set([table, index, ...keys], value)
	// 	}
	// }
}

function remove(table: keyof Schema, id: string) {
	const existing = db.get([table, "byId", id])
	if (!existing) return

	db.delete([table, "byId", id])

	// const tableIndexes = indexes[table]
	// if (tableIndexes) {
	// 	for (const [index, columns] of Object.entries(tableIndexes)) {
	// 		if (index === "byId") continue
	// 		const keys = columns.map((col) => existing[col])
	// 		db.delete([table, index, ...keys])
	// 	}
	// }
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}

const chet: Schema["user"] = { id: randomId(), name: "Chet" }
const simon: Schema["user"] = { id: randomId(), name: "Simon" }
const rob: Schema["user"] = { id: randomId(), name: "Rob" }

insert("user", chet)
insert("user", simon)
insert("user", rob)

const general: Schema["channel"] = { id: randomId(), name: "General" }
const engineering: Schema["channel"] = { id: randomId(), name: "Engineering" }
const marketing: Schema["channel"] = { id: randomId(), name: "Marketing" }

insert("channel", general)
insert("channel", engineering)
insert("channel", marketing)

const message1: Schema["message"] = {
	id: randomId(),
	text: "Hello world!",
	created_at: new Date().toISOString(),
	channel_id: general.id,
	author_id: chet.id,
}

const message2: Schema["message"] = {
	id: randomId(),
	text: "What's up?",
	created_at: new Date(Date.now() + 1000).toISOString(),
	channel_id: general.id,
	author_id: simon.id,
}

insert("message", message1)
insert("message", message2)

// Get a record by id.
assert.deepEqual(db.get(["user", "byId", rob.id]), rob)

// Scan an index.
assert.deepEqual(
	db
		.list({
			// Prefix scan.
			gt: ["user", "byName", "Simon", jsonCodec.MIN],
			lt: ["user", "byName", "Simon", jsonCodec.MAX],
		})
		.map(({ value }) => value),
	[simon]
)

assert.deepEqual(
	db
		.list({
			// Prefix scan.
			gt: ["user", "byName", "Simon", jsonCodec.MIN],
			lt: ["user", "byName", "Simon", jsonCodec.MAX],
		})
		.map(({ value }) => value),
	[simon]
)

// List messages, get the latest.
assert.deepEqual(
	db
		.list({
			gt: ["message", "byChannel", general.id, jsonCodec.MIN],
			lt: ["message", "byChannel", general.id, jsonCodec.MAX],
			reverse: true,
			limit: 1,
		})
		.map(({ value }) => value),
	[message2]
)

let called = 0
db.subscribe(
	[
		["message", "byChannel", general.id, jsonCodec.MIN],
		["message", "byChannel", general.id, jsonCodec.MAX],
	],
	() => {
		called += 1
	}
)

const message3: Schema["message"] = {
	id: randomId(),
	text: "Testing out subscriptions",
	created_at: new Date(Date.now() + 2000).toISOString(),
	channel_id: general.id,
	author_id: chet.id,
}

insert("message", message3)

assert.deepEqual(called, 1)

remove("message", message3.id)

assert.deepEqual(called, 2)
