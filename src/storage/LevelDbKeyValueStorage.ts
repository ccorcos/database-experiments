import { AbstractBatch } from "abstract-leveldown"
import { Level } from "level"
import { AsyncKeyValueStorage } from "../lib/AsyncBinaryPlusTree"

export class LevelDbKeyValueStorage<V> implements AsyncKeyValueStorage<V> {
	/**
	 * import { Level } from "level"
	 * new LevelDbKeyValueStorage(new Level("path/to.db"))
	 */
	constructor(public db: Level) {}

	get = async (key: string) => {
		try {
			const value = await this.db.get(key)
			return JSON.parse(value) as V
		} catch (error) {}
	}

	write = async (tx: {
		set?: { key: string; value: V }[]
		delete?: string[]
	}) => {
		const ops = [
			...(tx.delete || []).map(
				(key) =>
					({
						type: "del",
						key: key,
					} as AbstractBatch)
			),
			...(tx.set || []).map(
				({ key, value }) =>
					({
						type: "put",
						key: key,
						value: JSON.stringify(value),
					} as AbstractBatch)
			),
		]

		await this.db.batch(ops)
	}

	async close(): Promise<void> {
		return this.db.close()
	}
}
