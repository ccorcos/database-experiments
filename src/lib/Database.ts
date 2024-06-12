import { RWLock } from "@ccorcos/lock"

class Database {
	lock = new RWLock()
}

// export type KeyValueApi<K = string, V = any> = {
// 	get: (key: K) => V | undefined
// 	write: (tx: { set?: { key: K; value: V }[]; delete?: K[] }) => void
// }
// export type AsyncKeyValueApi<K = string, V = any> = AsyncApi<KeyValueApi<K, V>>

// export type OrderedKeyValueApi<K = string, V = any> = KeyValueApi<K, V> & {
// 	list(args?: {
// 		gt?: K
// 		gte?: K
// 		lt?: K
// 		lte?: K
// 		limit?: number
// 		reverse?: boolean
// 	}): { key: K; value: V }[]
// }

// export type AsyncOrderedKeyValueApi<K = string, V = any> = AsyncApi<
// 	OrderedKeyValueApi<K, V>
// >

// export type ReducerTreeApi<K, V, D> = OrderedKeyValueApi<K, V> & {
// 	reduce(args?: { gt?: K; gte?: K; lt?: K; lte?: K }): D
// }

// export type AsyncReducerTreeApi<K, V, D> = AsyncApi<
// 	ReducerTreeApi<KeyValueApi, V, D>
// >

// export type IntervalTreeApi<K extends [B, B, ...any[]], V, B> = ReducerTreeApi<
// 	K,
// 	V,
// 	[B, B]
// > & {
// 	overlaps(args?: { gt?: B; gte?: B; lt?: B; lte?: B }): { key: K; value: V }[]
// }

// export type AsyncIntervalTreeApi<K extends [B, B, ...any[]], V, B> = AsyncApi<
// 	IntervalTreeApi<K, V, B>
// >

// type Api = { [K: string]: (...args: any[]) => any }

// type AsyncApi<T extends Api> = {
// 	[K in keyof T]: (...args: Parameters<T[K]>) => Promise<ReturnType<T[K]>>
// }
