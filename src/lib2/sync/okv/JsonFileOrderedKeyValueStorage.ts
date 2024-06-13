import * as fs from "fs-extra"
import * as path from "path"
import { InMemoryOrderedKeyValueStorage } from "./InMemoryOrderedKeyValueStorage"
import { OrderedKeyValueApi } from "./types"

export class JsonFileOrderedKeyValueStorage
	extends InMemoryOrderedKeyValueStorage<string, string>
	implements OrderedKeyValueApi<string, string>
{
	constructor(public dbPath: string) {
		super()
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
		this.data = JSON.parse(contents) || []
	}

	private saveFile() {
		const contents = JSON.stringify(this.data)
		fs.mkdirpSync(path.dirname(this.dbPath))
		fs.writeFileSync(this.dbPath, contents, "utf8")
	}

	batch(writes: { set?: { key: string; value: string }[]; delete?: string[] }) {
		super.batch(writes)
		this.saveFile()
	}
}
