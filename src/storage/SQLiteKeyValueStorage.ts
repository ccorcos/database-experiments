import { Database, Statement, Transaction } from "better-sqlite3"
import { AsyncKeyValueStorage } from "../lib/AsyncBinaryPlusTree"

export class SQLiteKeyValueStorage<V> implements AsyncKeyValueStorage<V> {
	/**
	 * import sqlite from "better-sqlite3"
	 * new SQLiteKeyValueStorage(sqlite("path/to.db"))
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

	get = async (key: string) => {
		return this.getQuery
			.all({ key })
			.map((row: any) => JSON.parse(row.value))[0]
	}

	write = async (tx: {
		set?: { key: string; value: V }[]
		delete?: string[]
	}) => {
		this.writeFactsQuery(tx)
	}

	close() {
		this.db.close()
	}
}
