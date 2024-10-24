// Transaction ids are an incrementing number and are stored in a log.

import { insert, remove, search } from "@ccorcos/ordered-array"
import { Database, Statement, Transaction } from "better-sqlite3"
import { Codec, jsonCodec } from "lexicodec"

// Ideas and assumptions:
// - tuple keys and json values
// - an object is just {[key: string]: string | number | boolean}
// - transaction operations set and delete keys on the the json objects.
// - transactions ids are just an incrementing number.
// - missing data will throw a promise like suspense.

// We can easily extend this to file storage or localstorage...

export type KeyValueApi<K = any, V = any> = {
	get: (key: K) => V | undefined
	set: (key: K, value: V) => void
	delete: (key: K) => void
	write: (tx: { set?: { key: K; value: V }[]; delete?: K[] }) => void
}

export type OrderedKeyValueApi<K = any, V = any> = KeyValueApi<K, V> & {
	list(args?: {
		gt?: K
		gte?: K
		lt?: K
		lte?: K
		limit?: number
		reverse?: boolean
	}): { key: K; value: V }[]
}

export type IntervalTreeApi<
	B = (string | number)[],
	K = (string | number)[],
	V = any,
> = KeyValueApi<[B, B, K], V> & {
	overlaps: (args?: {
		gt?: B
		gte?: B
		lt?: B
		lte?: B
		limit?: number
		reverse?: boolean
	}) => { key: [B, B, K]; value: V }[]
}

function compare(a: any, b: any) {
	if (a === b) return 0
	if (a > b) return 1
	return -1
}

class InMemoryDatabase<K = any, V = any> implements OrderedKeyValueApi<K, V> {
	data: { key: K; value: V }[] = []

	constructor(public compareKey: (a: K, b: K) => number = compare) {}

	get(key: K) {
		const result = search(this.data, key, ({ key }) => key, this.compareKey)
		if (result.found === undefined) return
		return this.data[result.found].value
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
	): { key: K; value: V }[] {
		if (args.gt !== undefined && args.gte !== undefined)
			throw new Error("Invalid bounds: {gt, gte}")
		if (args.lt !== undefined && args.lte !== undefined)
			throw new Error("Invalid bounds: {lt, lte}")

		const start = args.gt ?? args.gte
		const startOpen = args.gt !== undefined
		const end = args.lt ?? args.lte
		const endOpen = args.lt !== undefined

		if (start !== undefined && end !== undefined) {
			const comp = this.compareKey(start, end)
			if (comp > 0) {
				console.warn("Invalid bounds.", args)
				return []
			}
			if (comp === 0 && (startOpen || endOpen)) {
				console.warn("Invalid bounds.", args)
				return []
			}
		}

		if (this.data.length === 0) return []

		let startIndex = 0
		if (start !== undefined) {
			const result = search(this.data, start, ({ key }) => key, this.compareKey)
			if (result.found !== undefined) {
				if (startOpen) startIndex = result.found + 1
				else startIndex = result.found
			} else startIndex = result.closest
		}

		let endIndex = this.data.length
		if (end !== undefined) {
			const result = search(this.data, end, ({ key }) => key, this.compareKey)
			if (result.found !== undefined) {
				if (endOpen) endIndex = result.found
				else endIndex = result.found + 1
			} else endIndex = result.closest
		}

		const result = this.data.slice(startIndex, endIndex)
		if (args.reverse) result.reverse()
		if (args.limit) result.splice(args.limit, result.length)
		return result
	}

	set(key: K, value: V) {
		this.write({ set: [{ key, value }] })
	}

	delete(key: K) {
		this.write({ delete: [key] })
	}

	write(tx: { set?: { key: K; value: V }[]; delete?: K[] }) {
		for (const { key, value } of tx.set || []) {
			insert(this.data, { key, value }, ({ key }) => key, this.compareKey)
		}
		for (const key of tx.delete || []) {
			remove(this.data, key, ({ key }) => key, this.compareKey)
		}
	}
}

class SQLiteDatabase<K = any, V = any> implements OrderedKeyValueApi<K, V> {
	/**
	 * import sqlite from "better-sqlite3"
	 * new SQLiteOKV(sqlite("path/to.db"))
	 */
	constructor(
		private db: Database,
		public codec: Codec = jsonCodec
	) {
		const createTableQuery = db.prepare(
			`create table if not exists data ( key text primary key, value text)`
		)

		// Make sure the table exists.
		createTableQuery.run()

		this.getQuery = db.prepare(`select * from data where key = $key`)

		const insertQuery = db.prepare(
			`insert or replace into data values ($key, $value)`
		)
		const deleteQuery = db.prepare(`delete from data where key = $key`)

		this.writeFactsQuery = this.db.transaction(
			(tx: { set?: { key: K; value: V }[]; delete?: K[] }) => {
				for (const { key, value } of tx.set || []) {
					insertQuery.run({
						key: this.codec.encode(key),
						value: this.codec.encode(value),
					})
				}
				for (const key of tx.delete || []) {
					deleteQuery.run({ key: this.codec.encode(key) })
				}
			}
		)
	}

	private getQuery: Statement
	private writeFactsQuery: Transaction

	get(key: K) {
		return this.getQuery
			.all({ key: this.codec.encode(key) })
			.map((row: any) => this.codec.decode(row.value))[0] as V | undefined
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
		const sqlArgs: any = {}
		const whereClauses: string[] = []

		if (args.gte !== undefined) {
			sqlArgs.gte = this.codec.encode(args.gte)
			whereClauses.push("key >= $gte")
		} else if (args.gt !== undefined) {
			sqlArgs.gt = this.codec.encode(args.gt)
			whereClauses.push("key > $gt")
		}

		if (args.lte !== undefined) {
			sqlArgs.lte = this.codec.encode(args.lte)
			whereClauses.push("key <= $lte")
		} else if (args.lt !== undefined) {
			sqlArgs.lt = this.codec.encode(args.lt)
			whereClauses.push("key < $lt")
		}

		let sqlQuery = `select * from data`
		if (whereClauses.length) {
			sqlQuery += " where "
			sqlQuery += whereClauses.join(" and ")
		}

		sqlQuery += " order by key"
		if (args.reverse) {
			sqlQuery += " desc"
		}
		if (args.limit) {
			sqlArgs.limit = args.limit
			sqlQuery += ` limit $limit`
		}

		const results: any[] = this.db.prepare(sqlQuery).all(sqlArgs)

		return results.map(({ key, value }) => ({
			key: this.codec.decode(key),
			value: this.codec.decode(value),
		}))
	}

	set(key: K, value: V) {
		this.write({ set: [{ key, value }] })
	}

	delete(key: K) {
		this.write({ delete: [key] })
	}

	write(tx: { set?: { key: K; value: V }[]; delete?: K[] }) {
		this.writeFactsQuery(tx)
	}

	close() {
		this.db.close()
	}
}
