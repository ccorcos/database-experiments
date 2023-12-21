import { orderedArray } from "@ccorcos/ordered-array"
import { ulid } from "ulid"

type Item = { key: string; value: any; version: string }
const getKey = (item: Item) => item.key
const { search, insert, update, remove } = orderedArray(getKey)

export class ConflictError extends Error {}

export class OrderedKeyValueDatabase {
	private data: { key: string; value: any; version: string }[] = []

	get = (key: string): { value: any; version: string } | undefined => {
		const result = search(this.data, key)
		if (result.found === undefined) return
		const { value, version } = this.data[result.found]
		return { value, version }
	}

	/**
	 * start is inclusive. end is exclusive. prefix is exclusive
	 */
	list = (args: {
		prefix?: string
		start?: string
		end?: string
		limit?: number
		reverse?: boolean
	}) => {
		let startKey: string | undefined
		let endKey: string | undefined
		if (args.prefix) {
			startKey = args.prefix + "\x00"
			endKey = args.prefix + "\xff"
		}
		if (args.start) {
			startKey = args.start
		}
		if (args.end) {
			endKey = args.end
		}

		if (startKey && endKey && startKey > endKey) {
			throw new Error("Invalid bounds.")
		}

		let startIndex: number = 0
		let endIndex: number = this.data.length - 1

		if (startKey) {
			const _start = startKey
			const result = search(this.data, _start)
			startIndex = result.found !== undefined ? result.found : result.closest
		}

		if (endKey) {
			const _end = endKey
			const result = search(this.data, _end)
			endIndex = result.found !== undefined ? result.found : result.closest
		}

		if (args.reverse) {
			if (!args.limit) return this.data.slice(startIndex, endIndex).reverse()
			return this.data
				.slice(Math.max(startIndex, endIndex - args.limit), endIndex)
				.reverse()
		}

		if (!args.limit) return this.data.slice(startIndex, endIndex)
		return this.data.slice(
			startIndex,
			Math.min(startIndex + args.limit, endIndex)
		)
	}

	write(tx: {
		check?: { key: string; version: string }[]
		// TODO: check range
		set?: { key: string; value: any }[]
		sum?: { key: string; value: number }[]
		min?: { key: string; value: number }[]
		max?: { key: string; value: number }[]
		delete?: string[]
		// TODO: delete range
	}) {
		for (const { key, version } of tx.check || [])
			if (this.get(key)?.version !== version)
				throw new ConflictError(`Version check failed. ${key} ${version}`)

		const version = ulid()

		for (const { key, value } of tx.set || [])
			insert(this.data, { key, value, version })

		const replaceValue = (key: string, fn: (existing?: any) => any) =>
			update(this.data, key, (item) => ({
				key,
				version,
				value: fn(item?.value),
			}))

		for (const { key, value } of tx.sum || [])
			replaceValue(key, (existing) => {
				if (typeof existing === "number") return existing + value
				if (existing === undefined) return value
				console.warn("Calling sum on a non-number value:", key, existing)
				return value
			})
		for (const { key, value } of tx.min || [])
			replaceValue(key, (existing) => {
				if (typeof existing === "number") return Math.min(existing, value)
				if (existing === undefined) return value
				console.warn("Calling min on a non-number value:", key, existing)
				return value
			})
		for (const { key, value } of tx.max || [])
			replaceValue(key, (existing) => {
				if (typeof existing === "number") return Math.max(existing, value)
				if (existing === undefined) return value
				console.warn("Calling max on a non-number value:", key, existing)
				return value
			})

		for (const key of tx.delete || []) remove(this.data, key)
	}
}
