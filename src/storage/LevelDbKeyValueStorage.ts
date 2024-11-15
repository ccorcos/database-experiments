import { AbstractBatch } from "abstract-leveldown"
import { Level } from "level"
import { AsyncKeyValueApi } from "../lib/types"

export class LevelDbKeyValueStorage<V = any>
	implements AsyncKeyValueApi<string, V>
{
	/**
	 * import { Level } from "level"
	 * new LevelDbKeyValueStorage(new Level("path/to.db"))
	 */
	constructor(public db: Level) {}

	async get(key: string) {
		try {
			const value = await this.db.get(key)
			return JSON.parse(value) as V
		} catch (error) {}
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
