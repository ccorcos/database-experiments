import sqlite, { Database } from "better-sqlite3"
import * as fs from "fs-extra"
import { Level } from "level"
import { jsonCodec } from "lexicodec"
import { max, min, sample, sum, uniq } from "lodash"
import Bench from "tinybench"
import { AsyncIntervalTree } from "./lib/AsyncIntervalTree"
import { AsyncReducerTree, TreeReducer } from "./lib/AsyncReducerTree"
import { printTable } from "./perfTools"
import { LevelDbKeyValueStorage } from "./storage/LevelDbKeyValueStorage"

let now = Date.now()
function tmp(fileName: string) {
	const dirPath = __dirname + "../tmp/" + now++
	fs.mkdirpSync(dirPath)
	return dirPath + "/" + fileName
}

const count: TreeReducer<any, any, number> = {
	leaf: (values) => values.length,
	branch: (children) => sum(children.map((child) => child.data)),
}

const sumReducer: TreeReducer<any, any, number> = {
	leaf: (values) => sum(values.map((x) => x.value)),
	branch: (children) => sum(children.map((child) => child.data)),
}

const maxValue: TreeReducer<any, number, number> = {
	leaf: (values) => max(values.map((v) => v.value))!,
	branch: (children) => max(children.map((child) => child.data))!,
}

const minValue: TreeReducer<any, number, number> = {
	leaf: (values) => min(values.map((v) => v.value))!,
	branch: (children) => min(children.map((child) => child.data))!,
}

async function test0() {
	// count reducer tree

	let tree: AsyncReducerTree
	let db: Database
	let ranges: [number, number][]

	const bench = new Bench({
		time: 2000,
		iterations: 2,
		setup: async () => {
			const numbers = randomInts(2000, [-100_000, 100_000])
			ranges = Array(0, 100)
				.fill(0)
				.map(
					() =>
						[sample(numbers)!, sample(numbers)!].sort((a, b) => a - b) as [
							number,
							number
						]
				)
			const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
			tree = new AsyncReducerTree(storage, 1, 40, sumReducer)
			await tree.write({ set: numbers.map((n) => ({ key: n, value: n })) })

			db = sqlite(tmp("data.sqlite"))
			const createTableQuery = db.prepare(
				`create table if not exists data ( key int primary key, value int)`
			)
			createTableQuery.run()
			const insertQuery = db.prepare(
				`insert or replace into data values ($key, $value)`
			)
			const writeQuery = db.transaction(
				(tx: { set?: { key: number; value: number }[] }) => {
					for (const { key, value } of tx.set || []) {
						insertQuery.run({ key, value })
					}
				}
			)
			writeQuery({ set: numbers.map((n) => ({ key: n, value: n })) })
		},
	})

	bench.add(`b+reducer on leveldb sum`, async () => {
		for (const [gte, lte] of ranges) await tree.reduce({ gte, lte })
	})

	bench.add(`sqlite sum`, async () => {
		const countQuery = db.prepare(
			`select sum(value) as total from data where key >= $gte and key <= $lte`
		)
		for (const [gte, lte] of ranges) countQuery.all({ gte, lte })
	})

	await bench.warmup()
	await bench.run()

	printTable(bench)

	// ┌─────────┬──────────────┬────────────────────────────┬──────────┬──────────┬─────────┐
	// │ (index) │ Average Time │ Task Name                  │ ops/sec  │ Margin   │ Samples │
	// ├─────────┼──────────────┼────────────────────────────┼──────────┼──────────┼─────────┤
	// │ 0       │ '099.692μs'  │ 'b+reducer on leveldb sum' │ '10,030' │ '±0.35%' │ 20062   │
	// │ 1       │ '254.870μs'  │ 'sqlite sum'               │ '3,923'  │ '±0.38%' │ 7848    │
	// └─────────┴──────────────┴────────────────────────────┴──────────┴──────────┴─────────┘
}

async function test1() {
	let tree: AsyncIntervalTree<[string, string, string], number>
	let db: Database
	let ranges: [string, string][]

	const bench = new Bench({
		time: 2000,
		iterations: 2,
		setup: async () => {
			const items: { key: [string, string, string]; value: number }[] = []
			const bounds = new Set<string>()

			const size = 2_000

			let i = 0
			while (i < size) {
				const [min, max] = [randomId(), randomId()].sort()
				bounds.add(min)
				bounds.add(max)
				items.push({
					key: [min, max, randomId()],
					value: i++,
				})

				// Same bound
				if (i % 7 === 0) {
					items.push({
						key: [min, max, randomId()],
						value: i++,
					})
				}

				// Half same bound
				if (i % 11 === 0) {
					const [min2, max2] = [min, randomId()].sort()
					bounds.add(min2)
					bounds.add(max2)
					items.push({
						key: [min2, max2, randomId()],
						value: i++,
					})
				}
			}

			// Also add some values that aren't in the dataset.
			for (let i = 0; i < size / 2; i++) bounds.add(randomId())

			// Random range queries for overlap
			ranges = Array(0, 100)
				.fill(0)
				.map(
					() => [sample(bounds)!, sample(bounds)!].sort() as [string, string]
				)

			const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
			tree = new AsyncIntervalTree(
				storage,
				1,
				40,
				jsonCodec.compare,
				jsonCodec.compare
			)
			await tree.write({ set: items })

			db = sqlite(tmp("data.sqlite"))
			const createTableQuery = db.prepare(
				`create table data ( id text primary key, lower text, upper text, value int)`
			)
			createTableQuery.run()
			const createIndexQuery = db.prepare(
				`create index idx_data on data ( lower, upper, id )`
			)
			createIndexQuery.run()
			const insertQuery = db.prepare(
				`insert or replace into data values ($id, $lower, $upper, $value)`
			)
			const writeQuery = db.transaction(
				(tx: { set?: { key: [string, string, string]; value: number }[] }) => {
					for (const {
						key: [lower, upper, id],
						value,
					} of tx.set || []) {
						insertQuery.run({ lower, upper, id, value })
					}
				}
			)
			writeQuery({ set: items })
		},
	})

	bench.add(`interval tree on leveldb`, async () => {
		for (const [gte, lte] of ranges) await tree.overlaps({ gte, lte })
	})

	bench.add(`sqlite overlaps`, async () => {
		const overlapsQuery = db.prepare(
			`select id from data where upper >= $gte and lower <= $lte`
		)
		for (const [gte, lte] of ranges) overlapsQuery.all({ gte, lte })
	})

	await bench.warmup()
	await bench.run()

	printTable(bench)
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}

function randomNumber(range: [number, number]) {
	return Math.random() * (range[1] - range[0]) + range[0]
}

function randomNumbers(size: number, range?: [number, number]) {
	if (!range) range = [-size * 10, size * 10]
	const numbers: number[] = []
	for (let i = 0; i < size; i++) numbers.push(randomNumber(range))
	return numbers
}

function randomInts(size: number, range?: [number, number]) {
	return uniq(randomNumbers(size, range).map((n) => Math.round(n)))
}

test1()
