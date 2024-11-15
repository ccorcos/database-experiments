import { IDBPDatabase, openDB } from "idb"
import { AsyncKeyValueApi } from "../lib/types"

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

	async write(writes: {
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

	async close() {
		const db = await this.db
		db.close()
	}
}
