import { IDBPDatabase, openDB } from "idb"
import {
	AsyncKeyValueApi,
	AsyncKeyValueReadTx,
	AsyncKeyValueWriteTx,
} from "./types"

const version = 1
const storeName = "kv"

export class IndexedDbKeyValueStorage<V = any>
	implements AsyncKeyValueApi<string, V>
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

	async batch(writes: {
		set?: { key: string; value: V }[]
		delete?: string[]
	}) {
		const db = await this.db
		const tx = db.transaction(storeName, "readwrite")
		for (const { key, value } of writes.set || []) {
			await tx.store.put(value, key)
		}
		for (const key of writes.delete || []) {
			await tx.store.delete(key)
		}
		await tx.done
	}

	read() {
		let promise = this.db.then((db) => db.transaction(storeName, "readonly"))

		return new AsyncKeyValueReadTx<V>({
			async get(key) {
				const tx = await promise
				const value = await tx.store.get(key)
				return value
			},
			async commit() {
				const tx = await promise
				await tx.done
			},
		})
	}

	write() {
		let promise = this.db.then((db) => db.transaction(storeName, "readwrite"))

		return new AsyncKeyValueWriteTx<V>({
			async get(key) {
				const tx = await promise
				const value = await tx.store.get(key)
				return value
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
