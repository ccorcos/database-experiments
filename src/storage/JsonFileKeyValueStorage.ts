import * as fs from "fs-extra"
import * as path from "path"
import { AsyncKeyValueStorage } from "../lib/AsyncBinaryPlusTree"

export class JsonFileKeyValueStorage<V> implements AsyncKeyValueStorage<V> {
	map: { [key: string]: V } = {}

	constructor(public dbPath: string) {
		this.loadFile()
	}

	private loadFile() {
		// Check that the file exists.
		try {
			const stat = fs.statSync(this.dbPath)
			if (!stat.isFile()) {
				throw new Error("Database is not a file.")
			}
		} catch (error) {
			if (error.code === "ENOENT") {
				// File does not exist.
				return
			}
			throw error
		}

		// Read the file.
		const contents = fs.readFileSync(this.dbPath, "utf8")
		this.map = JSON.parse(contents)
	}

	private saveFile() {
		const contents = JSON.stringify(this.map)
		fs.mkdirpSync(path.dirname(this.dbPath))
		fs.writeFileSync(this.dbPath, contents, "utf8")
	}

	get = async (key: string) => {
		return this.map[key]
	}

	write = async (tx: {
		set?: { key: string; value: V }[]
		delete?: string[]
	}) => {
		for (const { key, value } of tx.set || []) {
			this.map[key] = value
		}
		for (const key of tx.delete || []) {
			delete this.map[key]
		}
		this.saveFile()
	}
}
