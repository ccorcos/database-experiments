import { strict as assert } from "assert"
import sqlite from "better-sqlite3"
import { Level } from "level"
import { uniq } from "lodash"
import { describe, it } from "mocha"
import { AsyncKeyValueStorage } from "../lib/AsyncBinaryPlusTree"
import { IndexedDbKeyValueStorage } from "./IndexedDbKeyValueStorage"
import { JsonFileKeyValueStorage } from "./JsonFileKeyValueStorage"
import { LevelDbKeyValueStorage } from "./LevelDbKeyValueStorage"
import { SQLiteKeyValueStorage } from "./SQLiteKeyValueStorage"

async function propertyTest(storage: AsyncKeyValueStorage) {
	const numbers = randomNumbers(2_000)

	// Write them all.
	await storage.write({
		set: numbers.map((n) => ({ key: n.toString(), value: n })),
	})

	for (const n of numbers) {
		const before = await storage.get(n.toString())
		assert.equal(before, n)
		await storage.write({ delete: [n.toString()] })
		const after = await storage.get(n.toString())
		assert.equal(after, undefined)
	}
}

describe("Storage", () => {
	const now = Date.now()
	const dirPath = __dirname + "/../../tmp/" + now
	// fs.mkdirpSync(dirPath)

	it("JsonFileKeyValueStorage", async () => {
		const storage = new JsonFileKeyValueStorage(dirPath + "/data.json")
		await propertyTest(storage)
	})

	it("SQLiteKeyValueStorage", async () => {
		const storage = new SQLiteKeyValueStorage(sqlite(dirPath + "/data.sqlite"))
		await propertyTest(storage)
	})

	it("LevelDbKeyValueStorage", async () => {
		const storage = new LevelDbKeyValueStorage(
			new Level(dirPath + "/data.leveldb")
		)
		await propertyTest(storage)
	})

	it("IndexedDbKeyValueStorage", async () => {
		require("fake-indexeddb/auto")
		const storage = new IndexedDbKeyValueStorage(now.toString())
		await propertyTest(storage)
	})
})

function randomNumbers(size: number, range?: [number, number]) {
	if (!range) range = [-size * 10, size * 10]
	const numbers: number[] = []
	for (let i = 0; i < size; i++)
		numbers.push(Math.round(Math.random() * (range[1] - range[0]) - range[0]))
	return uniq(numbers)
}
