export type AsyncKeyValueApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>

	set: (key: K, value: V) => Promise<void>
	delete: (key: K) => Promise<void>
	batch: (writes: {
		set?: { key: K; value: V }[]
		delete?: K[]
	}) => Promise<void>

	read: () => AsyncKeyValueReadTxApi<K, V>
	write: () => AsyncKeyValueWriteTxApi<K, V>
}

export type AsyncKeyValueReadTxApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>
	commit: () => Promise<void>
}

export type AsyncKeyValueWriteTxApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>

	set: (key: K, value: V) => void
	delete: (key: K) => void
	batch: (writes: { set?: { key: K; value: V }[]; delete?: K[] }) => void

	commit: () => Promise<void>
}

export class AsyncKeyValueReadTx<V>
	implements AsyncKeyValueReadTxApi<string, V>
{
	constructor(
		private args: {
			get: (key: string) => Promise<V>
			commit: () => Promise<void>
		}
	) {}

	cache: { [key: string]: V | undefined } = {}

	async get(key: string) {
		if (key in this.cache) return this.cache[key]
		const value = await this.args.get(key)
		this.cache[key] = value
		return value
	}

	async commit() {
		await this.args.commit()
	}
}

export class AsyncKeyValueWriteTx<V>
	implements AsyncKeyValueWriteTxApi<string, V>
{
	constructor(
		private args: {
			get: (key: string) => Promise<V>
			commit: (writes: {
				set?: { key: string; value: V }[]
				delete?: string[]
			}) => Promise<void>
		}
	) {}

	cache: { [key: string]: V | undefined } = {}
	sets: { [key: string]: V } = {}
	deletes = new Set<string>()

	async get(key: string) {
		if (key in this.cache) return this.cache[key]
		const value = await this.args.get(key)
		this.cache[key] = value
		return value
	}

	set(key: string, value: V) {
		return this.batch({ set: [{ key, value }] })
	}

	delete(key: string) {
		return this.batch({ delete: [key] })
	}

	batch(writes: { set?: { key: string; value: V }[]; delete?: string[] }) {
		for (const { key, value } of writes.set || []) {
			this.sets[key] = value
			this.deletes.delete(key)
			this.cache[key] = value
		}
		for (const key of writes.delete || []) {
			delete this.sets[key]
			this.deletes.add(key)
			this.cache[key] = undefined
		}
	}

	async commit() {
		await this.args.commit({
			set: Object.entries(this.sets).map(([key, value]) => ({ key, value })),
			delete: Array.from(this.deletes),
		})
	}
}
