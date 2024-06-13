export type KeyValueApi<K = string, V = any> = {
	get: (key: K) => V | undefined

	set: (key: K, value: V) => void
	delete: (key: K) => void
	batch: (writes: {
		set?: { key: string; value: V }[]
		delete?: string[]
	}) => void
}
