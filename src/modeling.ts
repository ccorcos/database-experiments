// Starting a new idea from scratch.

// Data model for a messaging app using an ordered key-value store with tuple keys.

import { cloneDeep, get, isPlainObject, set, unset } from "lodash"
import * as t from "./examples/dataTypes"

const UserId = t.tuple(t.literal("user"), t.uuid)
const MessageId = t.tuple(t.literal("message"), t.uuid)
const ThreadId = t.tuple(t.literal("thread"), t.uuid)

// All objects in the key-value store have an id property which is the tuple key.
const UserSchema = t.object({
	id: UserId,
	version: t.number,
	username: t.string,
})

const MessageSchema = t.object({
	id: MessageId,
	version: t.number,
	text: t.string,
	createdAt: t.datetime,
	createdBy: UserId,
	threadId: t.tuple(t.literal("thread"), t.uuid),
})

const ThreadSchema = t.object({
	id: ThreadId,
	version: t.number,
	participants: t.array(UserId),
	subject: t.string,
	repliedAt: t.datetime,
})

const UserThreadsVersionId = t.tuple(t.literal("userThreadsVersion"), UserId)
const UserThreadsVersionSchema = t.object({ id: UserThreadsVersionId, version: t.number })

// userId, repliedAt, threadId
const UserThreadsId = t.tuple(t.literal("userThreads"), UserId, t.datetime, ThreadId)
const UserThreadsSchema = t.object({ id: UserThreadsId })

const UserThreadsIndex = {
	listen: ThreadSchema,
	map: (thread: t.Infer<typeof ThreadSchema>) => {
		return thread.participants.map((participant) => {
			const userThreads: t.Infer<typeof UserThreadsSchema> = {
				id: ["userThreads", participant, thread.repliedAt, thread.id],
			}
			return userThreads
		})
	},
}

const ThreadMessagesVersionId = t.tuple(t.literal("threadMessagesVersion"), ThreadId)
const ThreadMessagesVersion = t.object({ id: ThreadMessagesVersionId, version: t.number })

// threadId, createdAt, messageId
const ThreadMessagesId = t.tuple(t.literal("threadMessages"), ThreadId, t.datetime, MessageId)
const ThreadMessagesSchema = t.object({ id: ThreadMessagesId })

const ThreadMessageListIndex = {
	listen: MessageSchema,
	map: (message: t.Infer<typeof MessageSchema>) => {
		const threadMessageList: t.Infer<typeof ThreadMessagesSchema> = {
			id: ["threadMessages", message.threadId, message.createdAt, message.id],
		}
		return threadMessageList
	},
}

const RecordSchemas = [UserSchema, MessageSchema, ThreadSchema]

const IndexSchemas = [
	{ version: UserThreadsVersionSchema, list: UserThreadsSchema },
	{ version: ThreadMessagesVersion, list: ThreadMessagesSchema },
]

// NEXT
// - model, especially for lists.
// - model for editing values with persisted the history of edits

type SetOperation = {
	id: any[]
	type: "set"
	path: (string | number)[]
	value: any
}

type RemoveOperation = {
	id: any[]
	type: "remove"
	path: (string | number)[]
}

type InsertOperation = {
	id: any[]
	type: "insert"
	/** 0 to the beginning, -1 to the end */
	path: (string | number)[]
	value: any
}

type Operation = SetOperation | RemoveOperation | InsertOperation

type Writes<K, V> = { set?: { key: K; value: V }[]; delete?: K[] }

type KeyValueApi<K = (string | number)[], V = any> = {
	get: (key: K) => V | undefined
	set: (key: K, value: V) => void
	delete: (key: K) => void
}

type OrderedKeyValueApi<K = (string | number)[], V = any> = KeyValueApi<K, V> & {
	list: (args?: { gt?: K; gte?: K; lt?: K; lte?: K; limit?: number; reverse?: boolean }) => { key: K; value: V }[]
}

type IntervalTreeApi<B = (string | number)[], K = (string | number)[], V = any> = KeyValueApi<[B, B, K], V> & {
	overlaps: (args?: {
		gt?: B
		gte?: B
		lt?: B
		lte?: B
		limit?: number
		reverse?: boolean
	}) => Promise<{ key: [B, B, K]; value: V }[]>
}

// Whats the goal here?
// DONT FOREGET THE FILING CABINET METAPHOR
// - keep track of history of operations
// - increment the version of records as they're written
// - maintain indexes

function write(db: OrderedKeyValueApi<any, any>, ops: Operation[]) {
	return performOperations(
		{
			...db,
			set: (key, value) => {
				// TODO: validate schema
				// TODO: increment record version
				// TODO: maintain indexes
				db.set(key, value)
			},
			delete: (key) => {
				db.delete(key)
			},
		},
		ops
	)
}

function performOperations(db: OrderedKeyValueApi<any, any>, ops: Operation[]) {
	for (const op of ops) {
		if (op.type === "set") {
			const obj = db.get(op.id) || {}
			if (op.path.length === 0) {
				if (!isPlainObject(op.value)) throw new Error("Operation must be a plain object")
				db.set(op.id, { ...obj, ...op.value })
				continue
			}

			const newObj = set(cloneDeep(obj), op.path, op.value)
			db.set(op.id, newObj)
			continue
		}

		if (op.type === "remove") {
			const obj = db.get(op.id)
			if (!obj) continue

			if (op.path.length === 0) {
				db.delete(op.id)
				continue
			}

			const newObj = unset(cloneDeep(obj), op.path)
			db.set(op.id, newObj)
			continue
		}

		if (op.type === "insert") {
			const obj = db.get(op.id) || {}

			const arrayPath = op.path.slice(0, -1)
			let array = get(obj, arrayPath)

			const index = op.path[op.path.length - 1]

			// Initialize the array if it doesn't exist
			if (array === null || array === undefined) {
				array = []
			} else if (!Array.isArray(array)) {
				throw new Error("Insert operation can only be performed on arrays")
			}

			const newArray = [...array.slice(0, index), op.value, ...array.slice(index)]
			const newObj = set(cloneDeep(obj), arrayPath, newArray)
			db.set(op.id, newObj)
			continue
		}

		console.error("Unknown operation", op)
	}
}
