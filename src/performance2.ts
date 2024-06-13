import sqlite from "better-sqlite3"
import * as fs from "fs-extra"
import { Level } from "level"
import { chunk, sampleSize, shuffle } from "lodash"
import { Bench } from "tinybench"
import { AsyncBinaryPlusTree } from "./lib/AsyncBinaryPlusTree"
import { InMemoryBinaryPlusTree } from "./lib/InMemoryBinaryPlusTree"
import { LevelDbKeyValueStorage } from "./lib2/async/kv/LevelDbKeyValueStorage"
import { SQLiteKeyValueStorage } from "./lib2/sync/kv/SQLiteKeyValueStorage"
import { printTable } from "./perfTools"
/*

Performance...
- bptree with sqlite vs bptree with leveldb vs sqlite vs leveldb
- sqlite vs tuple bptree


minisql
- create table
- create index on table
- insert into table
- select from index

*/

let count = Date.now()
function tmp(fileName: string) {
	const dirPath = __dirname + "../tmp/" + count++
	fs.mkdirpSync(dirPath)
	return dirPath + "/" + fileName
}

async function test0() {
	const numbers = shuffle(
		Array(100_000)
			.fill(0)
			.map((x, i) => i)
	)

	const insertNumbers1 = sampleSize(numbers, 10_000)
	const readNumbers1 = sampleSize(numbers, 2000)
	const deleteNumbers = sampleSize(numbers, 4000)
	const insertNumbers2 = sampleSize(numbers, 1000)
	const readNumbers2 = sampleSize(numbers, 4000)

	async function writeReadDelete(tree: AsyncBinaryPlusTree) {
		for (const numbers of chunk(insertNumbers1, 1000))
			await tree.write({ set: numbers.map((n) => ({ key: n, value: n })) })

		for (const number of readNumbers1) await tree.get(number)

		for (const numbers of chunk(deleteNumbers, 100))
			await tree.write({ delete: numbers })

		for (const numbers of chunk(insertNumbers2, 1000))
			await tree.write({ set: numbers.map((n) => ({ key: n, value: n })) })

		for (const number of readNumbers2) await tree.get(number)
	}

	const bench = new Bench({ time: 2000, iterations: 2 })

	function sizeTest(min: number, max: number) {
		bench.add(`b+level ${min}-${max}`, async () => {
			const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
			const tree = new AsyncBinaryPlusTree(storage, min, max)
			await writeReadDelete(tree)
		})
	}

	sizeTest(4, 9)
	sizeTest(10, 20)
	sizeTest(1, 20)
	sizeTest(20, 40)
	sizeTest(1, 40)
	sizeTest(40, 80)
	sizeTest(1, 80)

	// sizeTest(50, 100)
	// sizeTest(1, 100)
	// sizeTest(10, 100)
	// sizeTest(20, 100)

	// sizeTest(100, 200)
	// sizeTest(1, 200)
	// sizeTest(10, 200)
	// sizeTest(40, 200)

	// sizeTest(200, 400)
	// sizeTest(10, 400)

	// sizeTest(400, 800)
	// sizeTest(800, 1600)
	// sizeTest(2000, 4000)
	// sizeTest(10000, 20000)

	await bench.warmup()
	await bench.run()

	printTable(bench)

	// ┌─────────┬──────────────┬───────────────────────┬─────────┬───────────┬─────────┐
	// │ (index) │ Average Time │ Task Name             │ ops/sec │ Margin    │ Samples │
	// ├─────────┼──────────────┼───────────────────────┼─────────┼───────────┼─────────┤
	// │ 0       │ '504.696ms'  │ 'b+level 50-100'      │ '1'     │ '±5.88%'  │ 4       │
	// │ 1       │ '474.645ms'  │ 'b+level 100-200'     │ '2'     │ '±3.55%'  │ 5       │
	// │ 2       │ '579.890ms'  │ 'b+level 200-400'     │ '1'     │ '±3.01%'  │ 4       │
	// │ 3       │ '828.546ms'  │ 'b+level 400-800'     │ '1'     │ '±4.55%'  │ 3       │
	// │ 4       │ '001.358s'   │ 'b+level 800-1600'    │ '0'     │ '±11.91%' │ 2       │
	// │ 5       │ '002.426s'   │ 'b+level 2000-4000'   │ '0'     │ '±2.71%'  │ 2       │
	// │ 6       │ '009.078s'   │ 'b+level 10000-20000' │ '0'     │ '±3.50%'  │ 2       │
	// └─────────┴──────────────┴───────────────────────┴─────────┴───────────┴─────────┘
}

async function test1() {
	const numbers = shuffle(
		Array(10_000)
			.fill(0)
			.map((x, i) => i)
	)
	const bench = new Bench({ time: 2000, iterations: 2 })
	bench
		.add("insert 10_000 memory", () => {
			const tree = new InMemoryBinaryPlusTree(50, 100)
			for (const n of numbers) tree.set(n, n)
		})
		.add("insert 10_000 sqlite", async () => {
			const storage = new SQLiteKeyValueStorage(sqlite(tmp("data.sqlite")))
			for (const n of numbers)
				await storage.batch({ set: [{ key: n.toString(), value: n }] })
		})
		.add("insert 10_000 b+sqlite", async () => {
			const storage = new SQLiteKeyValueStorage(sqlite(tmp("data.sqlite")))
			const tree = new AsyncBinaryPlusTree(storage, 1, 40)
			for (const n of numbers) await tree.set(n, n)
		})
		.add("insert 10_000 level", async () => {
			const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
			for (const n of numbers)
				await storage.write({ set: [{ key: n.toString(), value: n }] })
		})
		.add("insert 10_000 b+level", async () => {
			const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
			const tree = new AsyncBinaryPlusTree(storage, 1, 40)
			for (const n of numbers) await tree.set(n, n)
		})
		.add("insert batch 10_000 sqlite", async () => {
			const storage = new SQLiteKeyValueStorage(sqlite(tmp("data.sqlite")))
			await storage.batch({
				set: numbers.map((n) => ({ key: n.toString(), value: n })),
			})
		})
		.add("insert batch 10_000 b+sqlite", async () => {
			const storage = new SQLiteKeyValueStorage(sqlite(tmp("data.sqlite")))
			const tree = new AsyncBinaryPlusTree(storage, 1, 40)
			await tree.write({
				set: numbers.map((n) => ({ key: n.toString(), value: n })),
			})
		})
		.add("insert batch 10_000 level", async () => {
			const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
			await storage.write({
				set: numbers.map((n) => ({ key: n.toString(), value: n })),
			})
		})
		.add("insert batch 10_000 b+level", async () => {
			const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
			const tree = new AsyncBinaryPlusTree(storage, 1, 40)
			await tree.write({
				set: numbers.map((n) => ({ key: n.toString(), value: n })),
			})
		})

	await bench.warmup()
	await bench.run()

	printTable(bench)
}

async function test2() {
	const baseArray = shuffle(
		Array(100_000)
			.fill(0)
			.map((x, i) => i)
	)

	let storageSqlite: SQLiteKeyValueStorage
	let bpSqlite: AsyncBinaryPlusTree
	let storageLevel: LevelDbKeyValueStorage
	let bpLevel: AsyncBinaryPlusTree

	const bench = new Bench({
		time: 2000,
		iterations: 2,
		setup: async () => {
			{
				storageSqlite = new SQLiteKeyValueStorage(sqlite(tmp("data.sqlite")))

				await storageSqlite.batch({
					set: baseArray.map((n) => ({ key: n.toString(), value: n })),
				})
			}
			{
				const storage = new SQLiteKeyValueStorage(sqlite(tmp("data.sqlite")))
				bpSqlite = new AsyncBinaryPlusTree(storage, 1, 40)
				await bpSqlite.write({
					set: baseArray.map((n) => ({ key: n.toString(), value: n })),
				})
			}
			{
				storageLevel = new LevelDbKeyValueStorage(
					new Level(tmp("data.leveldb"))
				)
				await storageLevel.write({
					set: baseArray.map((n) => ({ key: n.toString(), value: n })),
				})
			}
			{
				const storage = new LevelDbKeyValueStorage(
					new Level(tmp("data.leveldb"))
				)
				bpLevel = new AsyncBinaryPlusTree(storage, 1, 40)
				await bpLevel.write({
					set: baseArray.map((n) => ({ key: n.toString(), value: n })),
				})
			}
		},
	})

	const insertNumbers = sampleSize(baseArray, 1000).map((n) => n + 0.5)
	const deleteNumbers = sampleSize(baseArray, 1000)
	const readNumbers = sampleSize(baseArray, 1000)

	bench
		.add("insert 1000 more from 100k sqlite", async () => {
			for (const n of insertNumbers)
				await storageSqlite.batch({ set: [{ key: n.toString(), value: n }] })
		})
		.add("insert 1000 more from 100k b+ sqlite", async () => {
			for (const n of insertNumbers)
				await bpSqlite.write({ set: [{ key: n.toString(), value: n }] })
		})
		.add("insert 1000 more from 100k level", async () => {
			for (const n of insertNumbers)
				await storageLevel.write({ set: [{ key: n.toString(), value: n }] })
		})
		.add("insert 1000 more from 100k b+ level", async () => {
			for (const n of insertNumbers)
				await bpLevel.write({ set: [{ key: n.toString(), value: n }] })
		})
		.add("delete 1000 more from 100k sqlite", async () => {
			for (const n of deleteNumbers)
				await storageSqlite.batch({ delete: [n.toString()] })
		})
		.add("delete 1000 more from 100k b+ sqlite", async () => {
			for (const n of deleteNumbers)
				await bpSqlite.write({ delete: [n.toString()] })
		})
		.add("delete 1000 more from 100k level", async () => {
			for (const n of deleteNumbers)
				await storageLevel.write({ delete: [n.toString()] })
		})
		.add("delete 1000 more from 100k b+ level", async () => {
			for (const n of deleteNumbers)
				await bpLevel.write({ delete: [n.toString()] })
		})
		.add("read 1000 from 100k sqlite", async () => {
			for (const n of readNumbers) await storageSqlite.get(n.toString())
		})
		.add("read 1000 from 100k b+ sqlite", async () => {
			for (const n of readNumbers) await bpSqlite.get(n.toString())
		})
		.add("read 1000 from 100k level", async () => {
			for (const n of readNumbers) await storageLevel.get(n.toString())
		})
		.add("read 1000 from 100k b+ level", async () => {
			for (const n of readNumbers) await bpLevel.get(n.toString())
		})

	await bench.warmup()
	await bench.run()

	printTable(bench)
}

test2()
