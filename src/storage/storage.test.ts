import { search } from "@ccorcos/ordered-array"
import { strict as assert } from "assert"
import sqlite from "better-sqlite3"
import { Level } from "level"
import { uniq } from "lodash"
import { describe, it } from "mocha"
import {
	AsyncKeyValueApi,
	AsyncOrderedKeyValueApi,
	KeyValueApi,
	OrderedKeyValueApi,
} from "../lib/types"
import { IndexedDbKeyValueStorage } from "./IndexedDbKeyValueStorage"
import { IndexedDbOrderedKeyValueStorage } from "./IndexedDbOrderedKeyValueStorage"
import { JsonFileKeyValueStorage } from "./JsonFileKeyValueStorage"
import { JsonFileOrderedKeyValueStorage } from "./JsonFileOrderedKeyValueStorage"
import { LevelDbKeyValueStorage } from "./LevelDbKeyValueStorage"
import { LevelDbOrderedKeyValueStorage } from "./LevelDbOrderedKeyValueStorage"
import { SQLiteKeyValueStorage } from "./SQLiteKeyValueStorage"
import { SQLiteOrderedKeyValueStorage } from "./SQLiteOrderedKeyValueStorage"

async function keyValuePropertyTest(storage: AsyncKeyValueApi | KeyValueApi) {
	const numbers = randomNumbers(2_000)

	// Write them all.
	await storage.write({
		set: numbers.map((n) => ({ key: n.toString(), value: n })),
	})

	// Delete them all.
	for (const n of numbers) {
		const before = await storage.get(n.toString())
		assert.equal(before, n)
		await storage.write({ delete: [n.toString()] })
		const after = await storage.get(n.toString())
		assert.equal(after, undefined)
	}
}

async function orderedKeyValuePropertyTest(
	storage: AsyncOrderedKeyValueApi | OrderedKeyValueApi
) {
	await keyValuePropertyTest(storage)

	const toKey = (n: number) => n.toString().padStart(5, "0")

	// Write some even numbers.
	const numbers = Array(1000)
		.fill(0)
		.map((_, i) => toKey(i * 2))

	await storage.write({
		set: numbers.map((n) => ({ key: n, value: n })),
	})

	const listTest = async (start: number, end: number) => {
		const options: any[][] = [
			[
				{ gt: toKey(start) },
				{ gte: toKey(start) },
				{ gt: toKey(start - 1) },
				{ gte: toKey(start - 1) },
			],
			[
				{ lt: toKey(end) },
				{ lte: toKey(end) },
				{ lt: toKey(end + 1) },
				{ lte: toKey(end + 1) },
			],
			[{}, { reverse: true }],
			[{}, { limit: 1 }, { limit: 10 }],
		]

		for (const combination of permuteOptions(options)) {
			const args = Object.assign({}, ...combination)
			if (args.gt >= args.lt) continue
			if (args.gte >= args.lt) continue
			if (args.gt >= args.lte) continue
			if (args.gte > args.lte) continue

			const queryResult = (await storage.list(args)).map(({ key }) => key)

			const start = args.gt ?? args.gte
			const startOpen = args.gt !== undefined
			const end = args.lt ?? args.lte
			const endOpen = args.lt !== undefined

			let startIndex = 0
			if (start !== undefined) {
				const result = search(numbers, start)
				if (result.found !== undefined) {
					if (startOpen) startIndex = result.found + 1
					else startIndex = result.found
				} else startIndex = result.closest
			}

			let endIndex = numbers.length
			if (end !== undefined) {
				const result = search(numbers, end)
				if (result.found !== undefined) {
					if (endOpen) endIndex = result.found
					else endIndex = result.found + 1
				} else endIndex = result.closest
			}

			const expectedResult = numbers.slice(startIndex, endIndex)
			if (args.reverse) expectedResult.reverse()
			if (args.limit) expectedResult.splice(args.limit, expectedResult.length)

			assert.deepEqual(queryResult, expectedResult, JSON.stringify(args))
		}
	}

	await listTest(0, 0)

	await listTest(-10, -4)
	await listTest(0, 1)
	await listTest(0, 2)
	await listTest(2, 2)
	await listTest(5000, 5020)
	await listTest(-10, 10)
	await listTest(0, 10)
	await listTest(1980, 2010)

	for (let i = 0; i < 100; i++) {
		const [start, end] = randomNumbers(2, [-100, 2100]).sort()
		await listTest(start, end)
	}
}

let log = false

describe("KeyValueStorage", () => {
	const now = Date.now()
	const dirPath = __dirname + "/../../tmp/" + now
	// fs.mkdirpSync(dirPath)

	it("JsonFileKeyValueStorage", async () => {
		const storage = new JsonFileKeyValueStorage(dirPath + "/data2.json")
		await keyValuePropertyTest(storage)
	})

	it("SQLiteKeyValueStorage", async () => {
		const storage = new SQLiteKeyValueStorage(sqlite(dirPath + "/data2.sqlite"))
		await keyValuePropertyTest(storage)
	})

	it("LevelDbKeyValueStorage", async () => {
		const storage = new LevelDbKeyValueStorage(
			new Level(dirPath + "/data2.leveldb")
		)
		await keyValuePropertyTest(storage)
	})

	it("IndexedDbKeyValueStorage", async () => {
		require("fake-indexeddb/auto")
		const storage = new IndexedDbKeyValueStorage(now.toString() + "2")
		await keyValuePropertyTest(storage)
	})
})

describe("OrderedKeyValueStorage", () => {
	const now = Date.now()
	const dirPath = __dirname + "/../../tmp/" + now
	// fs.mkdirpSync(dirPath)

	it("JsonFileOrderedKeyValueStorage", async () => {
		const storage = new JsonFileOrderedKeyValueStorage(dirPath + "/data.json")
		await orderedKeyValuePropertyTest(storage)
	})

	it("SQLiteOrderedKeyValueStorage", async () => {
		const storage = new SQLiteOrderedKeyValueStorage(
			sqlite(dirPath + "/data.sqlite")
		)
		await orderedKeyValuePropertyTest(storage)
	})

	it("LevelDbOrderedKeyValueStorage", async function () {
		this.timeout(10_000)
		const storage = new LevelDbOrderedKeyValueStorage(
			new Level(dirPath + "/data.leveldb")
		)
		await orderedKeyValuePropertyTest(storage)
	})

	it("IndexedDbOrderedKeyValueStorage", async function () {
		this.timeout(30_000)
		require("fake-indexeddb/auto")
		const storage = new IndexedDbOrderedKeyValueStorage(now.toString())
		await orderedKeyValuePropertyTest(storage)
	})
})

function randomNumbers(size: number, range?: [number, number]) {
	if (!range) range = [-size * 10, size * 10]
	const numbers: number[] = []
	for (let i = 0; i < size; i++)
		numbers.push(Math.round(Math.random() * (range[1] - range[0]) - range[0]))
	return uniq(numbers)
}

function permuteOptions<T>(options: T[][]): T[][] {
	if (options.length === 0) return []

	let result: T[][] = [[]]

	for (const group of options) {
		const expand: T[][] = []
		for (const combination of result) {
			for (const value of group) {
				expand.push([...combination, value])
			}
		}
		result = expand
	}

	return result
}

describe("permuteOptions", () => {
	it("permuteOptions", async () => {
		assert.deepEqual(permuteOptions([[1, 2, 3], [4, 5], [6], [7, 8]]), [
			[1, 4, 6, 7],
			[1, 4, 6, 8],
			[1, 5, 6, 7],
			[1, 5, 6, 8],
			[2, 4, 6, 7],
			[2, 4, 6, 8],
			[2, 5, 6, 7],
			[2, 5, 6, 8],
			[3, 4, 6, 7],
			[3, 4, 6, 8],
			[3, 5, 6, 7],
			[3, 5, 6, 8],
		])
	})
})
