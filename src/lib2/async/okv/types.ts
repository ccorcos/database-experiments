import { InMemoryOrderedKeyValueStorage } from "../../sync/okv/InMemoryOrderedKeyValueStorage"

export type ListArgs<K> = {
	gt?: K
	gte?: K
	lt?: K
	lte?: K
	limit?: number
	reverse?: boolean
}

export type AsyncOrderedKeyValueApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>
	list(args?: ListArgs<K>): Promise<{ key: K; value: V }[]>

	set: (key: K, value: V) => Promise<void>
	delete: (key: K) => Promise<void>
	batch: (writes: {
		set?: { key: K; value: V }[]
		delete?: string[]
	}) => Promise<void>

	read: () => AsyncOrderedKeyValueReadTxApi<K, V>
	write: () => AsyncOrderedKeyValueWriteTxApi<K, V>
}

export type AsyncOrderedKeyValueReadTxApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>
	list(args: ListArgs<K>): Promise<{ key: K; value: V }[]>
	commit: () => Promise<void>
}

export type AsyncOrderedKeyValueWriteTxApi<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>
	list(args?: ListArgs<K>): Promise<{ key: K; value: V }[]>

	set: (key: K, value: V) => void
	delete: (key: K) => void
	batch: (writes: { set?: { key: K; value: V }[]; delete?: K[] }) => void

	commit: () => Promise<void>
}

export class AsyncOrderedKeyValueReadTx<K, V>
	implements AsyncOrderedKeyValueReadTxApi<K, V>
{
	constructor(
		private args: {
			get: (key: K) => Promise<V>
			list(args: ListArgs<K>): Promise<{ key: K; value: V }[]>
			commit: () => Promise<void>
		}
	) {}

	async get(key: K) {
		return await this.args.get(key)
	}

	async list(args: ListArgs<K> = {}) {
		return await this.args.list(args)
	}

	async commit() {
		await this.args.commit()
	}
}

function compare(a: any, b: any) {
	if (a === b) return 0
	if (a > b) return 1
	return -1
}

export class AsyncOrderedKeyValueWriteTx<K, V>
	implements AsyncOrderedKeyValueWriteTxApi<K, V>
{
	constructor(
		private args: {
			get: (key: K) => Promise<V>
			list(args: ListArgs<K>): Promise<{ key: K; value: V }[]>
			commit: (writes: {
				set?: { key: K; value: V }[]
				delete?: K[]
			}) => Promise<void>
		},
		private compareKey: (a: K, b: K) => number = compare
	) {}

	sets = new InMemoryOrderedKeyValueStorage<K, V>(this.compareKey)
	deletes = new InMemoryOrderedKeyValueStorage<K, true>(this.compareKey)

	async get(key: K) {
		const alreadySet = this.sets.get(key)
		if (alreadySet !== undefined) return alreadySet

		const alreadyDeleted = this.deletes.get(key)
		if (alreadyDeleted) return undefined

		const value = await this.args.get(key)
		return value
	}

	async list(args: ListArgs<K> = {}) {
		const deletedItems = this.deletes.list({ ...args, limit: undefined })
		const newLimit =
			args.limit === undefined ? undefined : args.limit + deletedItems.length

		const results = new InMemoryOrderedKeyValueStorage<K, V>(this.compareKey)

		const existingItems = await this.args.list({ ...args, limit: newLimit })
		for (const { key, value } of existingItems) results.set(key, value)

		const newItems = this.sets.list(args)
		for (const { key, value } of newItems) results.set(key, value)

		for (const { key } of deletedItems) results.delete(key)

		return results.list(args)
	}

	set(key: K, value: V) {
		return this.batch({ set: [{ key, value }] })
	}

	delete(key: K) {
		return this.batch({ delete: [key] })
	}

	batch(writes: { set?: { key: K; value: V }[]; delete?: K[] }) {
		for (const { key, value } of writes.set || []) {
			this.sets.set(key, value)
			this.deletes.delete(key)
		}
		for (const key of writes.delete || []) {
			this.sets.delete(key)
			this.deletes.set(key, true)
		}
	}

	async commit() {
		await this.args.commit({
			set: this.sets.list(),
			delete: this.deletes.list().map(({ key }) => key),
		})
	}
}
