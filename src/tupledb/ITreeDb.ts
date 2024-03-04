import { insert, remove } from "@ccorcos/ordered-array"
import { jsonCodec } from "lexicodec"
import { InMemoryIntervalTree } from "../lib/InMemoryIntervalTree"

// No concurrency control because this is synchronous and embedded, just like SQLite.
export class ITreeDb<B = any, K = any, V = any> {
	constructor(
		public compareBound: (a: B, b: B) => number = jsonCodec.compare,
		public compareKey: (a: K, b: K) => number = jsonCodec.compare
	) {}

	compareTuple = (a: [B, B, K], b: [B, B, K]) => {
		let dir = this.compareBound(a[0], b[0])
		if (dir !== 0) return dir
		dir = this.compareBound(a[1], b[1])
		if (dir !== 0) return dir
		return this.compareKey(a[2], b[2])
	}

	data = new InMemoryIntervalTree<[B, B, K], V, B>(
		1,
		40,
		this.compareTuple,
		this.compareBound
	)

	listeners = new InMemoryIntervalTree<[B, B, string], () => void, B>(
		1,
		40,
		(a, b) => {
			let dir = this.compareBound(a[0], b[0])
			if (dir !== 0) return dir
			dir = this.compareBound(a[1], b[1])
			if (dir !== 0) return dir
			if (a[2] > b[2]) return 1
			if (a[2] < b[2]) return -1
			return 0
		},
		this.compareBound
	)

	get(key: [B, B, K]) {
		return this.data.get(key)
	}

	overlaps(args: { gt?: B; gte?: B; lt?: B; lte?: B } = {}) {
		return this.data.overlaps(args)
	}

	subscribe(range: [B, B], fn: () => void) {
		const id = randomId()
		this.listeners.set([...range, id], fn)
		return () => this.listeners.delete([...range, id])
	}

	set(key: [B, B, K], value: V) {
		return this.write({ sets: [{ key, value }] })
	}

	delete(key: [B, B, K]) {
		return this.write({ deletes: [key] })
	}

	write(args: {
		sets?: { key: [B, B, K]; value: V }[]
		deletes?: [B, B, K][]
	}) {
		const keys: [B, B, K][] = []

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
			const results = this.listeners.overlaps({ gte: key[0], lte: key[1] })
			for (const { value: fn } of results) fns.add(fn)
		}
		for (const fn of fns) fn()
	}

	transact() {
		return new ITreeTx(this)
	}
}

export class ITreeTx<B = any, K = any, V = any> {
	constructor(public db: ITreeDb<B, K, V>) {}

	sets = new InMemoryIntervalTree<[B, B, K], V, B>(
		1,
		40,
		this.db.compareTuple,
		this.db.compareBound
	)

	deletes = new InMemoryIntervalTree<[B, B, K], true, B>(
		1,
		40,
		this.db.compareTuple,
		this.db.compareBound
	)

	get(key: [B, B, K]) {
		const alreadyWritten = this.sets.get(key)
		if (alreadyWritten !== undefined) return alreadyWritten as V

		const alreadyDeleted = this.deletes.get(key)
		if (alreadyDeleted !== undefined) return undefined

		return this.db.data.get(key)
	}

	overlaps(args: { gt?: B; gte?: B; lt?: B; lte?: B } = {}) {
		const sets = this.sets.overlaps(args)
		const deletes = this.deletes.overlaps(args)

		const result = this.db.overlaps(args)

		for (const item of sets) {
			insert(result, item, ({ key }) => key, this.db.compareTuple)
		}

		for (const { key } of deletes) {
			remove(result, key, ({ key }) => key, this.db.compareTuple)
		}

		return result
	}

	delete(key: [B, B, K]) {
		this.sets.delete(key)
		this.deletes.set(key, true)
	}

	set(key: [B, B, K], value: V) {
		this.sets.set(key, value)
		this.deletes.delete(key)
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
