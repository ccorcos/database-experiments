import { ulid } from "ulid"

export class ConflictError extends Error {}

export class KeyValueDatabase<V = any> {
	map: { [key: string]: { value: V; version: string } } = {}

	get = (key: string): { value: V; version: string } | undefined => {
		const existing = this.map[key]
		if (existing) return existing
		else return undefined
	}

	write(tx: {
		check?: { key: string; version: string | undefined }[]
		set?: { key: string; value: V }[]
		delete?: string[]
		sum?: { key: string; value: number }[]
		min?: { key: string; value: number }[]
		max?: { key: string; value: number }[]
	}) {
		for (const { key, version } of tx.check || [])
			if (this.map[key]?.version !== version)
				throw new ConflictError(`Version check failed. ${key} ${version}`)

		const version = ulid()

		for (const { key, value } of tx.set || [])
			this.map[key] = { value, version }

		const replace = (key: string, update: (value?: any) => number) => {
			const existing = this.map[key]
			this.map[key] = { value: update(existing?.value) as any, version }
		}

		for (const { key, value } of tx.sum || [])
			replace(key, (existing) => {
				if (typeof existing === "number") return existing + value
				if (existing === undefined) return value
				console.warn("Calling sum on a non-number value:", key, existing)
				return value
			})
		for (const { key, value } of tx.min || [])
			replace(key, (existing) => {
				if (typeof existing === "number") return Math.min(existing, value)
				if (existing === undefined) return value
				console.warn("Calling min on a non-number value:", key, existing)
				return value
			})
		for (const { key, value } of tx.max || [])
			replace(key, (existing) => {
				if (typeof existing === "number") return Math.max(existing, value)
				if (existing === undefined) return value
				console.warn("Calling max on a non-number value:", key, existing)
				return value
			})

		for (const key of tx.delete || []) delete this.map[key]
	}
}
