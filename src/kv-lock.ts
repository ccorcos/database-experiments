import { RWLockMap } from "@rocicorp/lock"

export class AsyncKeyValueDatabase<T = any> {
	map = new Map<string, T>()

	async get(key: string) {
		return this.map.get(key)
	}

	async write(tx: { set?: { key: string; value: T }[]; delete?: string[] }) {
		for (const { key, value } of tx.set || []) this.map.set(key, value)
		for (const key of tx.delete || []) this.map.delete(key)
	}

	locks = new RWLockMap()

	transact() {
		return new AsyncKeyValueTransaction(this)
	}
}

export class AsyncKeyValueTransaction<T> {
	locks = new Set<() => void>()
	cache: { [key: string]: T | undefined } = {}
	sets: { [key: string]: T } = {}
	deletes = new Set<string>()

	constructor(private kv: AsyncKeyValueDatabase<T>) {}

	readLock = async (key: string) => {
		const release = await this.kv.locks.read(key)
		this.locks.add(release)
		return () => {
			this.locks.delete(release)
			release()
		}
	}

	writeLock = async (key: string) => {
		const release = await this.kv.locks.write(key)
		this.locks.add(release)
		return () => {
			this.locks.delete(release)
			release()
		}
	}

	async get(key: string): Promise<T | undefined> {
		if (key in this.cache) return this.cache[key]
		const value = await this.kv.get(key)
		this.cache[key] = value
		return value
	}

	set(key: string, value: T) {
		this.sets[key] = value
		this.cache[key] = value
		this.deletes.delete(key)
	}

	delete(key: string) {
		this.cache[key] = undefined
		delete this.sets[key]
		this.deletes.add(key)
	}

	release() {
		for (const release of this.locks) release()
	}

	async commit() {
		await this.kv.write({
			set: Object.entries(this.sets).map(([key, value]) => ({ key, value })),
			delete: Array.from(this.deletes),
		})
		this.release()
	}
}
