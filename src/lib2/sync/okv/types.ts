import { KeyValueApi } from "../kv/types"

export type OrderedKeyValueApi<K = string, V = any> = KeyValueApi<K, V> & {
	list(args?: {
		gt?: K
		gte?: K
		lt?: K
		lte?: K
		limit?: number
		reverse?: boolean
	}): { key: K; value: V }[]
}
