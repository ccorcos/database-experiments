import { insert, remove, search } from "@ccorcos/ordered-array"
import * as fs from "fs-extra"
import * as path from "path"
import { OrderedKeyValueApi } from "../lib/types"

function compare(a: any, b: any) {
	if (a === b) return 0
	if (a > b) return 1
	return -1
}

export class JsonFileOrderedKeyValueStorage<K = string, V = any>
	implements OrderedKeyValueApi<K, V>
{
	data: { key: K; value: V }[] = []

	constructor(
		public dbPath: string,
		public compareKey: (a: K, b: K) => number = compare
	) {
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

	write(tx: { set?: { key: K; value: V }[]; delete?: K[] }) {
		for (const { key, value } of tx.set || []) {
			insert(this.data, { key, value }, ({ key }) => key, this.compareKey)
		}
		for (const key of tx.delete || []) {
			remove(this.data, key, ({ key }) => key, this.compareKey)
		}
		this.saveFile()
	}
}
