import { AbstractBatch } from "abstract-leveldown"
import { Level } from "level"
import { AsyncOrderedKeyValueApi } from "../lib/types"

export class LevelDbOrderedKeyValueStorage<V = any>
	implements AsyncOrderedKeyValueApi<string, V>
{
	/**
	 * import { Level } from "level"
	 * new LevelDbOrderedKeyValueStorage(new Level("path/to.db"))
	 */
	constructor(public db: Level) {}

	async get(key: string) {
		try {
			const value = await this.db.get(key)
			return JSON.parse(value) as V
		} catch (error) {}
	}

	async list(
		args: {
			gt?: string
			gte?: string
			lt?: string
			lte?: string
			limit?: number
			reverse?: boolean
		} = {}
	) {
		const results: { key: string; value: V }[] = []
		for await (const [key, value] of this.db.iterator(args)) {
			results.push({ key: key, value: JSON.parse(value) })
		}
		return results
	}

	async write(
		writes: {
			set?: { key: string; value: V }[]
			delete?: string[]
		} = {}
	) {
		const ops: AbstractBatch[] = []

		for (const key of writes.delete || []) {
			ops.push({ type: "del", key: key })
		}
		for (const { key, value } of writes.set || []) {
			ops.push({
				type: "put",
				key: key,
				value: JSON.stringify(value),
			})
		}
		await this.db.batch(ops)
	}

	async close(): Promise<void> {
		return this.db.close()
	}
}
