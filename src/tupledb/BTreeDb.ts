import { insert, remove } from "@ccorcos/ordered-array"
import { jsonCodec } from "lexicodec"
import { InMemoryBinaryPlusTree } from "../lib/InMemoryBinaryPlusTree"
import { InMemoryIntervalTree } from "../lib/InMemoryIntervalTree"

export class BTreeDb<K = any, V = any> {
	constructor(public compareKey: (a: K, b: K) => number = jsonCodec.compare) {}

	data = new InMemoryBinaryPlusTree<K, V>(1, 40, this.compareKey)

	listeners = new InMemoryIntervalTree<[K, K, string], () => void, K>(
		1,
		40,
		(a, b) => {
			let dir = this.compareKey(a[0], b[0])
			if (dir !== 0) return dir
			dir = this.compareKey(a[1], b[1])
			if (dir !== 0) return dir
			if (a[2] > b[2]) return 1
			if (a[2] < b[2]) return -1
			return 0
		},
		this.compareKey
	)

	get(key: K) {
		return this.data.get(key)
	}

	list(
		args: {
			gt?: K
			gte?: K
			lt?: K
			lte?: K
			limit?: number
			reverse?: boolean
		} = {}
	) {
		return this.data.list(args)
	}

	subscribe(range: [K, K], fn: () => void) {
		const id = randomId()
		this.listeners.set([...range, id], fn)
		return () => this.listeners.delete([...range, id])
	}

	set(key: K, value: V) {
		return this.write({ sets: [{ key, value }] })
	}

	delete(key: K) {
		return this.write({ deletes: [key] })
	}

	write(args: { sets?: { key: K; value: V }[]; deletes?: K[] }) {
		const keys: K[] = []

		for (const { key, value } of args.sets || []) {
			this.data.set(key, value)
			keys.push(key)
		}

		for (const key of args.deletes || []) {
			this.data.delete(key)
			keys.push(key)
		}

		// Emit only once per caller.
		const fns = new Set<() => void>()
		for (const key of keys) {
			const results = this.listeners.overlaps({ gte: key, lte: key })
			for (const { value: fn } of results) fns.add(fn)
		}
		for (const fn of fns) fn()
	}

	transact() {
		return new BTreeTx(this)
	}
}

export class BTreeTx<K = any, V = any> {
	constructor(public db: BTreeDb<K, V>) {}

	sets = new InMemoryBinaryPlusTree<K, V>(1, 40, this.db.data.compareKey)
	deletes = new InMemoryBinaryPlusTree<K, true>(1, 40, this.db.data.compareKey)

	get(key: K) {
		const alreadyWritten = this.sets.get(key)
		if (alreadyWritten !== undefined) return alreadyWritten as V

		const alreadyDeleted = this.deletes.get(key)
		if (alreadyDeleted !== undefined) return undefined

		return this.db.data.get(key)
	}

	set(key: K, value: V) {
		this.sets.set(key, value)
		this.deletes.delete(key)
	}

	list(
		args: {
			gt?: K
			gte?: K
			lt?: K
			lte?: K
			limit?: number
			reverse?: boolean
		} = {}
	) {
		const sets = this.sets.list(args)
		const deletes = this.deletes.list(args)

		const limit =
			args.limit !== undefined ? args.limit + deletes.length : undefined

		const result = this.db.list({ ...args, limit })

		const compareKey = (a: K, b: K) => {
			const dir = this.db.compareKey(a, b) * -1
			if (args.reverse) return dir * -1
			else return dir
		}

		for (const item of sets) {
			insert(result, item, ({ key }) => key, compareKey)
		}

		for (const { key } of deletes) {
			remove(result, key, ({ key }) => key, compareKey)
		}

		if (args.limit && result.length > args.limit) {
			result.splice(args.limit, result.length)
		}

		return result
	}

	delete(key: K) {
		this.sets.delete(key)
		this.deletes.set(key, true)
	}

	commit() {
		this.db.write({
			sets: this.sets.list(),
			deletes: this.deletes.list().map(({ key }) => key),
		})
	}
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}
