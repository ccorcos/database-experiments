import { Database, Statement, Transaction } from "better-sqlite3"
import { OrderedKeyValueApi } from "../lib/types"

export class SQLiteOrderedKeyValueStorage<V = any>
	implements OrderedKeyValueApi<string, V>
{
	/**
	 * import sqlite from "better-sqlite3"
	 * new SQLiteOrderedKeyValueStorage(sqlite("path/to.db"))
	 */
	constructor(private db: Database) {
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
			(tx: { set?: { key: string; value: any }[]; delete?: string[] }) => {
				for (const { key, value } of tx.set || []) {
					insertQuery.run({ key, value: JSON.stringify(value) })
				}
				for (const key of tx.delete || []) {
					deleteQuery.run({ key: key })
				}
			}
		)
	}

	private getQuery: Statement
	private writeFactsQuery: Transaction

	get(key: string) {
		return this.getQuery
			.all({ key })
			.map((row: any) => JSON.parse(row.value))[0] as V | undefined
	}

	list(
		args: {
			gt?: string
			gte?: string
			lt?: string
			lte?: string
			limit?: number
			reverse?: boolean
		} = {}
	) {
		const sqlArgs: any = {}
		const whereClauses: string[] = []

		if (args.gte !== undefined) {
			sqlArgs.gte = args.gte
			whereClauses.push("key >= $gte")
		} else if (args.gt !== undefined) {
			sqlArgs.gt = args.gt
			whereClauses.push("key > $gt")
		}

		if (args.lte !== undefined) {
			sqlArgs.lte = args.lte
			whereClauses.push("key <= $lte")
		} else if (args.lt !== undefined) {
			sqlArgs.lt = args.lt
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
			key: key,
			value: JSON.parse(value),
		}))
	}

	write(tx: { set?: { key: string; value: V }[]; delete?: string[] }) {
		this.writeFactsQuery(tx)
	}

	close() {
		this.db.close()
	}
}
