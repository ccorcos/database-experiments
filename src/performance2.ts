import sqlite from "better-sqlite3"
import * as fs from "fs-extra"
import { Level } from "level"
import { sampleSize, shuffle } from "lodash"
import * as path from "path"
import { Bench } from "tinybench"
import { AsyncBinaryPlusTree } from "./lib/AsyncBinaryPlusTree"
import { InMemoryBinaryPlusTree } from "./lib/InMemoryBinaryPlusTree"
import { printTable } from "./perfTools"
import { LevelDbKeyValueStorage } from "./storage/LevelDbKeyValueStorage"
import { SQLiteKeyValueStorage } from "./storage/SQLiteKeyValueStorage"
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

async function test1() {
	let count = Date.now()
	const dirPath = __dirname + "../tmp/" + Date.now()

	const numbers = shuffle(
		Array(10_000)
			.fill(0)
			.map((x, i) => i)
	)
	const bench = new Bench({ time: 2000, iterations: 2 })
	bench
		.add("insert 10_000 memory", () => {
			const bpMemory = new InMemoryBinaryPlusTree(50, 100)
			for (const n of numbers) bpMemory.set(n, n)
		})
		.add("insert 10_000 sqlite", async () => {
			const dbPath = dirPath + `/${count++}/data.sqlite`
			fs.mkdirpSync(path.dirname(dbPath))

			const storage = new SQLiteKeyValueStorage(sqlite(dbPath))
			for (const n of numbers)
				await storage.write({ set: [{ key: n.toString(), value: n }] })
		})
		.add("insert 10_000 b+sqlite", async () => {
			const dbPath = dirPath + `/${count++}/data.sqlite`
			fs.mkdirpSync(path.dirname(dbPath))

			const bpSqlite = new AsyncBinaryPlusTree(
				new SQLiteKeyValueStorage(sqlite(dbPath)),
				50,
				100
			)
			for (const n of numbers) await bpSqlite.set(n, n)
		})
		.add("insert 10_000 level", async () => {
			const dbPath = dirPath + `/${count++}/data.leveldb`
			fs.mkdirpSync(path.dirname(dbPath))
			const storage = new LevelDbKeyValueStorage(new Level(dbPath))
			for (const n of numbers)
				await storage.write({ set: [{ key: n.toString(), value: n }] })
		})
		.add("insert 10_000 b+level", async () => {
			const dbPath = dirPath + `/${count++}/data.leveldb`
			fs.mkdirpSync(path.dirname(dbPath))

			const bpLevel = new AsyncBinaryPlusTree(
				new LevelDbKeyValueStorage(new Level(dbPath)),
				50,
				100
			)
			for (const n of numbers) await bpLevel.set(n, n)
		})

	await bench.warmup()
	await bench.run()

	printTable(bench)

	// ┌─────────┬──────────────┬──────────────────────────┬─────────┬───────────┬─────────┐
	// │ (index) │ Average Time │ Task Name                │ ops/sec │ Margin    │ Samples │
	// ├─────────┼──────────────┼──────────────────────────┼─────────┼───────────┼─────────┤
	// │ 0       │ '003.774ms'  │ 'insert 10_000 memory'   │ '264'   │ '±0.84%'  │ 531     │
	// │ 1       │ '002.643s'   │ 'insert 10_000 sqlite'   │ '0'     │ '±45.28%' │ 2       │
	// │ 2       │ '003.491s'   │ 'insert 10_000 b+sqlite' │ '0'     │ '±16.47%' │ 2       │
	// │ 3       │ '101.937ms'  │ 'insert 10_000 level'    │ '9'     │ '±4.28%'  │ 20      │
	// │ 4       │ '648.184ms'  │ 'insert 10_000 b+level'  │ '1'     │ '±7.02%'  │ 4       │
	// └─────────┴──────────────┴──────────────────────────┴─────────┴───────────┴─────────┘
}

const tmpPath = __dirname + "../tmp/"
async function test2() {
	let count = Date.now()

	const numbers = shuffle(
		Array(10_000)
			.fill(0)
			.map((x, i) => i)
	)
	const bench = new Bench({ time: 2000, iterations: 2 })
	bench
		.add("insert batch 10_000 sqlite", async () => {
			const dbPath = tmpPath + `/${count++}/data.sqlite`
			fs.mkdirpSync(path.dirname(dbPath))

			const storage = new SQLiteKeyValueStorage(sqlite(dbPath))
			await storage.write({
				set: numbers.map((n) => ({ key: n.toString(), value: n })),
			})
		})
		.add("insert batch 10_000 b+sqlite", async () => {
			const dbPath = tmpPath + `/${count++}/data.sqlite`
			fs.mkdirpSync(path.dirname(dbPath))

			const bpSqlite = new AsyncBinaryPlusTree(
				new SQLiteKeyValueStorage(sqlite(dbPath)),
				50,
				100
			)

			await bpSqlite.write({
				set: numbers.map((n) => ({ key: n.toString(), value: n })),
			})
		})
		.add("insert batch 10_000 level", async () => {
			const dbPath = tmpPath + `/${count++}/data.leveldb`
			fs.mkdirpSync(path.dirname(dbPath))
			const storage = new LevelDbKeyValueStorage(new Level(dbPath))
			await storage.write({
				set: numbers.map((n) => ({ key: n.toString(), value: n })),
			})
		})
		.add("insert batch 10_000 b+level", async () => {
			const dbPath = tmpPath + `/${count++}/data.leveldb`
			fs.mkdirpSync(path.dirname(dbPath))

			const bpLevel = new AsyncBinaryPlusTree(
				new LevelDbKeyValueStorage(new Level(dbPath)),
				50,
				100
			)
			await bpLevel.write({
				set: numbers.map((n) => ({ key: n.toString(), value: n })),
			})
		})

	await bench.warmup()
	await bench.run()

	printTable(bench)

	// ┌─────────┬──────────────┬────────────────────────────────┬─────────┬──────────┬─────────┐
	// │ (index) │ Average Time │ Task Name                      │ ops/sec │ Margin   │ Samples │
	// ├─────────┼──────────────┼────────────────────────────────┼─────────┼──────────┼─────────┤
	// │ 0       │ '013.698ms'  │ 'insert batch 10_000 sqlite'   │ '73'    │ '±0.70%' │ 147     │
	// │ 1       │ '014.422ms'  │ 'insert batch 10_000 b+sqlite' │ '69'    │ '±3.18%' │ 139     │
	// │ 2       │ '011.913ms'  │ 'insert batch 10_000 level'    │ '83'    │ '±1.90%' │ 168     │
	// │ 3       │ '014.032ms'  │ 'insert batch 10_000 b+level'  │ '71'    │ '±2.08%' │ 143     │
	// └─────────┴──────────────┴────────────────────────────────┴─────────┴──────────┴─────────┘
}

async function test3() {
	let count = Date.now()
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
				const dbPath = tmpPath + `/${count++}/data.sqlite`
				fs.mkdirpSync(path.dirname(dbPath))
				storageSqlite = new SQLiteKeyValueStorage(sqlite(dbPath))

				await storageSqlite.write({
					set: baseArray.map((n) => ({ key: n.toString(), value: n })),
				})
			}
			{
				const dbPath = tmpPath + `/${count++}/data.sqlite`
				fs.mkdirpSync(path.dirname(dbPath))
				const storage = new SQLiteKeyValueStorage(sqlite(dbPath))
				bpSqlite = new AsyncBinaryPlusTree(storage, 50, 100)
				await bpSqlite.write({
					set: baseArray.map((n) => ({ key: n.toString(), value: n })),
				})
			}
			{
				const dbPath = tmpPath + `/${count++}/data.leveldb`
				fs.mkdirpSync(path.dirname(dbPath))
				storageLevel = new LevelDbKeyValueStorage(new Level(dbPath))
				await storageLevel.write({
					set: baseArray.map((n) => ({ key: n.toString(), value: n })),
				})
			}
			{
				const dbPath = tmpPath + `/${count++}/data.leveldb`
				fs.mkdirpSync(path.dirname(dbPath))
				const storage = new LevelDbKeyValueStorage(new Level(dbPath))
				bpLevel = new AsyncBinaryPlusTree(storage, 50, 100)
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
				await storageSqlite.write({ set: [{ key: n.toString(), value: n }] })
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
				await storageSqlite.write({ delete: [n.toString()] })
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

	// ┌─────────┬──────────────┬────────────────────────────────────────┬─────────┬───────────┬─────────┐
	// │ (index) │ Average Time │ Task Name                              │ ops/sec │ Margin    │ Samples │
	// ├─────────┼──────────────┼────────────────────────────────────────┼─────────┼───────────┼─────────┤
	// │ 0       │ '282.322ms'  │ 'insert 1000 more from 100k sqlite'    │ '3'     │ '±3.59%'  │ 8       │
	// │ 1       │ '381.368ms'  │ 'insert 1000 more from 100k b+ sqlite' │ '2'     │ '±3.90%'  │ 6       │
	// │ 2       │ '009.899ms'  │ 'insert 1000 more from 100k level'     │ '101'   │ '±1.18%'  │ 203     │
	// │ 3       │ '087.145ms'  │ 'insert 1000 more from 100k b+ level'  │ '11'    │ '±1.11%'  │ 23      │
	// │ 4       │ '008.787ms'  │ 'delete 1000 more from 100k sqlite'    │ '113'   │ '±23.38%' │ 228     │
	// │ 5       │ '386.247ms'  │ 'delete 1000 more from 100k b+ sqlite' │ '2'     │ '±3.71%'  │ 6       │
	// │ 6       │ '009.846ms'  │ 'delete 1000 more from 100k level'     │ '101'   │ '±1.46%'  │ 204     │
	// │ 7       │ '086.948ms'  │ 'delete 1000 more from 100k b+ level'  │ '11'    │ '±2.66%'  │ 24      │
	// │ 8       │ '005.837ms'  │ 'read 1000 from 100k sqlite'           │ '171'   │ '±0.60%'  │ 343     │
	// │ 9       │ '051.422ms'  │ 'read 1000 from 100k b+ sqlite'        │ '19'    │ '±1.46%'  │ 39      │
	// │ 10      │ '006.396ms'  │ 'read 1000 from 100k level'            │ '156'   │ '±0.84%'  │ 313     │
	// │ 11      │ '055.136ms'  │ 'read 1000 from 100k b+ level'         │ '18'    │ '±0.58%'  │ 37      │
	// └─────────┴──────────────┴────────────────────────────────────────┴─────────┴───────────┴─────────┘
}

test3()
