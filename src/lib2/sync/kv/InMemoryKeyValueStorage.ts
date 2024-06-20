import { KeyValueApi } from "./types"

type K = string | number

export class InMemoryKeyValueStorage<V = any> implements KeyValueApi<K, V> {
	data = new Map<K, V>()

	constructor() {}

	get(key: K) {
		return this.data.get(key)
	}

	set(key: K, value: V) {
		this.batch({ set: [{ key, value }] })
	}

	delete(key: K) {
		this.batch({ delete: [key] })
	}

	batch(writes: { set?: { key: K; value: V }[]; delete?: K[] }) {
		for (const { key, value } of writes.set || []) {
			this.data.set(key, value)
		}
		for (const key of writes.delete || []) {
			this.data.delete(key)
		}
	}
}
