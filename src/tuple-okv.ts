import { orderedArray } from "@ccorcos/ordered-array"
import { Codec } from "lexicodec"
import { ulid } from "ulid"

type Tuple = any[]
type Item = { key: Tuple; value: any; version: string }

export class ConflictError extends Error {}

export class OrderedTupleValueDatabase {
	constructor(private codec: Codec) {}
	private utils = orderedArray((item: Item) => item.key, this.codec.compare)

	private data: { key: Tuple; value: any; version: string }[] = []

	get = (key: Tuple): { value: any; version: string } | undefined => {
		const result = this.utils.search(this.data, key)
		if (result.found === undefined) return
		const { value, version } = this.data[result.found]
		return { value, version }
	}

	/**
	 * start is inclusive. end is exclusive. prefix is exclusive
	 */
	list = (args: {
		prefix?: Tuple
		start?: Tuple
		end?: Tuple
		limit?: number
		reverse?: boolean
	}) => {
		let startKey: Tuple | undefined
		let endKey: Tuple | undefined
		if (args.prefix) {
			startKey = args.prefix
			endKey = [...args.prefix, this.codec.MAX]
		}
		if (args.start) {
			startKey = args.start
		}
		if (args.end) {
			endKey = args.end
		}

		if (startKey && endKey && this.codec.compare(startKey, endKey) > 0) {
			throw new Error("Invalid bounds.")
		}

		let startIndex: number = 0
		let endIndex: number = this.data.length - 1

		if (startKey) {
			const _start = startKey
			const result = this.utils.search(this.data, _start)
			if (result.found === undefined) {
				startIndex = result.closest
			} else if (startKey === args.prefix) {
				startIndex = result.found + 1
			} else {
				startIndex = result.found
			}
		}

		if (endKey) {
			const _end = endKey
			const result = this.utils.search(this.data, _end)
			if (result.found === undefined) {
				endIndex = result.closest
			} else {
				endIndex = result.found
			}
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
		check?: { key: Tuple; version: string }[]
		// TODO: check range
		set?: { key: Tuple; value: any }[]
		sum?: { key: Tuple; value: number }[]
		min?: { key: Tuple; value: number }[]
		max?: { key: Tuple; value: number }[]
		delete?: Tuple[]
		// TODO: delete range
	}) {
		for (const { key, version } of tx.check || [])
			if (this.get(key)?.version !== version)
				throw new ConflictError(`Version check failed. ${key} ${version}`)

		const version = ulid()

		for (const { key, value } of tx.set || [])
			this.utils.insert(this.data, { key, value, version })

		const replaceValue = (key: Tuple, fn: (existing?: any) => any) =>
			this.utils.update(this.data, key, (item) => ({
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

		for (const key of tx.delete || []) this.utils.remove(this.data, key)
	}
}
