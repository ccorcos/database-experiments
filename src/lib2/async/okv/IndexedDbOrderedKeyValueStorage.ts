import { IDBPDatabase, IDBPTransaction, openDB } from "idb"
import {
	AsyncOrderedKeyValueApi,
	AsyncOrderedKeyValueReadTx,
	AsyncOrderedKeyValueWriteTx,
	ListArgs,
} from "./types"

const version = 1
const storeName = "okv"

async function list<V>(
	tx: IDBPTransaction<any, [any], "readonly" | "readwrite">,
	args: ListArgs<string> = {}
) {
	const index = tx.store // primary key

	const start = args.gt ?? args.gte
	const startOpen = args.gt !== undefined
	const end = args.lt ?? args.lte
	const endOpen = args.lt !== undefined

	if (start !== undefined && end !== undefined) {
		if (start > end) {
			console.warn("Invalid bounds.", args)
			return []
		}
		if (start === end && (startOpen || endOpen)) {
			console.warn("Invalid bounds.", args)
			return []
		}
	}

	let range: IDBKeyRange | null
	if (end) {
		if (start) {
			range = IDBKeyRange.bound(start, end, startOpen, endOpen)
		} else {
			range = IDBKeyRange.upperBound(end, endOpen)
		}
	} else {
		if (start) {
			range = IDBKeyRange.lowerBound(start, startOpen)
		} else {
			range = null
		}
	}

	const direction: IDBCursorDirection = args?.reverse ? "prev" : "next"

	const limit = args?.limit || Infinity
	let results: { key: string; value: V }[] = []
	for await (const cursor of index.iterate(range, direction)) {
		results.push({ key: cursor.key, value: cursor.value })
		if (results.length >= limit) break
	}
	return results
}

export class IndexedDbOrderedKeyValueStorage<V = any>
	implements AsyncOrderedKeyValueApi<string, V>
{
	private db: Promise<IDBPDatabase<any>>

	constructor(public dbName: string) {
		this.db = openDB(dbName, version, {
			upgrade(db) {
				db.createObjectStore(storeName)
			},
		})
	}

	async get(key: string) {
		const db = await this.db
		const value = await db.get(storeName, key)
		return value as V | undefined
	}

	async set(key: string, value: V) {
		return this.batch({ set: [{ key, value }] })
	}

	async delete(key: string) {
		return this.batch({ delete: [key] })
	}

	async list(args: ListArgs<string> = {}) {
		const db = await this.db
		const tx = db.transaction(storeName, "readonly")
		const results = await list<V>(tx, args)
		await tx.done
		return results
	}

	async batch(writes: {
		set?: { key: string; value: V }[]
		delete?: string[]
	}) {
		const db = await this.db
		const tx = db.transaction(storeName, "readwrite")
		for (const { key, value } of writes.set || []) {
			tx.store.put(value, key)
		}
		for (const key of writes.delete || []) {
			tx.store.delete(key)
		}
		await tx.done
	}

	read() {
		let promise = this.db.then((db) => db.transaction(storeName, "readonly"))

		return new AsyncOrderedKeyValueReadTx<string, V>({
			async get(key) {
				const tx = await promise
				const value = await tx.store.get(key)
				return value
			},
			async list(args) {
				const tx = await promise
				const results = await list<V>(tx, args)
				return results
			},
			async commit() {
				const tx = await promise
				await tx.done
			},
		})
	}

	write() {
		let promise = this.db.then((db) => db.transaction(storeName, "readwrite"))

		return new AsyncOrderedKeyValueWriteTx<string, V>({
			async get(key) {
				const tx = await promise
				const value = await tx.store.get(key)
				return value
			},
			async list(args) {
				const tx = await promise
				const results = await list<V>(tx, args)
				return results
			},
			async commit(writes = {}) {
				const tx = await promise

				for (const { key, value } of writes.set || []) {
					await tx.store.put(value, key)
				}
				for (const key of writes.delete || []) {
					await tx.store.delete(key)
				}
				await tx.done
			},
		})
	}

	async close() {
		const db = await this.db
		db.close()
	}
}
