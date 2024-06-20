import { RWLock } from "@ccorcos/lock"
import { AbstractBatch } from "abstract-leveldown"
import { Level } from "level"
import {
	AsyncOrderedKeyValueApi,
	AsyncOrderedKeyValueReadTx,
	AsyncOrderedKeyValueWriteTx,
	ListArgs,
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

export class LevelDbOrderedKeyValueStorage
	implements AsyncOrderedKeyValueApi<string, string>
{
	/**
	 * import { Level } from "level"
	 * new LevelDbOrderedKeyValueStorage(new Level("path/to.db"))
	 */
	constructor(public db: Level) {}

	lock = new RWLock()

	async get(key: string) {
		const value = await this.db.get(key)
		return value
	}

	async list(args: ListArgs<string> = {}) {
		const results: { key: string; value: string }[] = []
		for await (const [key, value] of this.db.iterator(args)) {
			results.push({ key, value })
		}
		return results
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
		await this.db.batch(batchOps(writes))
	}

	read() {
		const { db } = this
		let promise = this.lock.read()

		return new AsyncOrderedKeyValueReadTx<string, string>({
			async get(key) {
				await promise
				return db.get(key)
			},
			async list(args) {
				const results: { key: string; value: string }[] = []
				for await (const [key, value] of db.iterator(args)) {
					results.push({ key, value })
				}
				return results
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

		return new AsyncOrderedKeyValueWriteTx<string, string>({
			async get(key) {
				await promise
				return db.get(key)
			},
			async list(args) {
				const results: { key: string; value: string }[] = []
				for await (const [key, value] of db.iterator(args)) {
					results.push({ key, value })
				}
				return results
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
