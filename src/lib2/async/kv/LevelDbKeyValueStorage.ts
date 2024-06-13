import { RWLock } from "@ccorcos/lock"
import { AbstractBatch } from "abstract-leveldown"
import { Level } from "level"
import {
	AsyncKeyValueApi,
	AsyncKeyValueReadTx,
	AsyncKeyValueWriteTx,
} from "./types"

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
		return this.lock.withRead(() => this._get(key))
	}

	async _get(key: string) {
		const value = await this.db.get(key)
		return value
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
		return this.lock.withWrite(() => this._batch(writes))
	}

	async _batch(
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
		await this.db.batch(ops)
	}

	read() {
		const db = this
		let promise = this.lock.read()

		return new AsyncKeyValueReadTx({
			async get(key) {
				await promise
				const value = await db._get(key)
				return value
			},
			async commit() {
				const release = await promise
				release()
			},
		})
	}

	write() {
		const db = this
		let promise = this.lock.write()

		return new AsyncKeyValueWriteTx({
			async get(key) {
				await promise
				const value = await db._get(key)
				return value
			},
			async commit(writes) {
				const release = await promise
				await db._batch(writes)
				release()
			},
		})
	}

	async close(): Promise<void> {
		return this.db.close()
	}
}
