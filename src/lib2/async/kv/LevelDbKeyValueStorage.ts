import { RWLock } from "@ccorcos/lock"
import { AbstractBatch } from "abstract-leveldown"
import { Level } from "level"
import {
	AsyncKeyValueApi,
	AsyncKeyValueReadTx,
	AsyncKeyValueWriteTx,
} from "./types"

function batchOps(
	writes: {
		set?: { key: string; value: string }[]
		delete?: string[]
	} = {}
) {
	const ops: AbstractBatch[] = []

	for (const key of writes.delete || []) {
		ops.push({ type: "del", key })
	}
	for (const { key, value } of writes.set || []) {
		ops.push({ type: "put", key, value })
	}
	return ops
}

export class LevelDbKeyValueStorage
	implements AsyncKeyValueApi<string, string>
{
	/**
	 * import { Level } from "level"
	 * new LevelDbKeyValueStorage(new Level("path/to.db"))
	 */
	constructor(public db: Level) {}

	lock = new RWLock()

	async get(key: string) {
		return this.lock.withRead(() => this.db.get(key))
	}

	async set(key: string, value: string) {
		return this.batch({ set: [{ key, value }] })
	}

	async delete(key: string) {
		return this.batch({ delete: [key] })
	}

	async batch(
		writes: {
			set?: { key: string; value: string }[]
			delete?: string[]
		} = {}
	) {
		return this.lock.withWrite(() => this.db.batch(batchOps(writes)))
	}

	read() {
		const { db } = this
		let promise = this.lock.read()

		return new AsyncKeyValueReadTx({
			async get(key) {
				await promise
				return db.get(key)
			},
			async commit() {
				const release = await promise
				release()
			},
		})
	}

	write() {
		const { db } = this
		let promise = this.lock.write()

		return new AsyncKeyValueWriteTx({
			async get(key) {
				await promise
				return db.get(key)
			},
			async commit(writes) {
				const release = await promise
				await db.batch(batchOps(writes))
				release()
			},
		})
	}

	async close(): Promise<void> {
		return this.db.close()
	}
}