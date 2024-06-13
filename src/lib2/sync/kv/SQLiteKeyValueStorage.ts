import { Database, Statement, Transaction } from "better-sqlite3"
import { KeyValueApi } from "./types"

export class SQLiteKeyValueStorage implements KeyValueApi<string, string> {
	private getQuery: Statement
	private insertQuery: Statement
	private deleteQuery: Statement
	private writeFactsQuery: Transaction

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

		this.insertQuery = db.prepare(
			`insert or replace into data values ($key, $value)`
		)
		this.deleteQuery = db.prepare(`delete from data where key = $key`)

		this.writeFactsQuery = this.db.transaction(
			(tx: { set?: { key: string; value: any }[]; delete?: string[] }) => {
				for (const { key, value } of tx.set || []) {
					this.insertQuery.run({ key, value })
				}
				for (const key of tx.delete || []) {
					this.deleteQuery.run({ key })
				}
			}
		)
	}

	get(key: string) {
		return this.getQuery.all({ key }).map((row: any) => row.value)[0] as
			| string
			| undefined
	}

	set(key: string, value: string) {
		return this.insertQuery.run({ key, value })
	}

	delete(key: string) {
		return this.deleteQuery.run({ key })
	}

	batch(writes: { set?: { key: string; value: string }[]; delete?: string[] }) {
		this.writeFactsQuery(writes)
	}

	close() {
		this.db.close()
	}
}
