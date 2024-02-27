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
	const dirPath = __dirname + "/../tmp/" + now++
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

			const size = 20_000

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

			// Random range queries for overlap
			const boundsArray = Array.from(bounds)
			ranges = Array(0, 100)
				.fill(0)
				.map(
					() =>
						[sample(boundsArray)!, sample(boundsArray)!].sort() as [
							string,
							string
						]
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

	bench.add(`random ranges interval tree on leveldb`, async () => {
		for (const [gte, lte] of ranges) await tree.overlaps({ gte, lte })
	})

	bench.add(`random ranges sqlite overlaps`, async () => {
		const overlapsQuery = db.prepare(
			`select id from data where upper >= $gte and lower <= $lte`
		)
		for (const [gte, lte] of ranges) overlapsQuery.all({ gte, lte })
	})

	await bench.warmup()
	await bench.run()

	printTable(bench)
}

async function test2() {
	let tree: AsyncIntervalTree<[string, string, string], number>
	let db: Database
	let ranges: [string, string][]

	const size = 100_000
	const overlapRatio = 0.02

	const bench = new Bench({
		time: 2000,
		iterations: 2,
		setup: async () => {
			const items: { key: [string, string, string]; value: number }[] = []

			const maxValue = size * 20
			const overlapSize = maxValue * overlapRatio

			let i = 0
			while (i < size) {
				// More realistic ranges that are spread at most 10% of the total range.
				const start = randomInt([0, maxValue])
				const end = start + randomInt([0, overlapSize])
				const min = start.toString().padStart(7, "0")
				const max = end.toString().padStart(7, "0")

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
					const end2 = start + randomInt([-overlapSize / 2, overlapSize / 2])
					const [min2, max2] = [min, end2.toString().padStart(7, "0")].sort()
					items.push({
						key: [min2, max2, randomId()],
						value: i++,
					})
				}
			}

			// Random range queries for overlap
			ranges = Array(0, 100)
				.fill(0)
				.map(() => {
					const start = randomInt([0, maxValue])
					const end = start + randomInt([0, overlapSize * 2])
					const min = start.toString().padStart(7, "0")
					const max = end.toString().padStart(7, "0")
					return [min, max]
				})

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

	bench.add(
		`${size} items, ${Math.round(
			overlapRatio * 100
		)}% ranges, interval tree on leveldb`,
		async () => {
			for (const [gte, lte] of ranges) await tree.overlaps({ gte, lte })
		}
	)

	bench.add(
		`${size} items, ${Math.round(overlapRatio * 100)}% ranges, sqlite overlaps`,
		async () => {
			const overlapsQuery = db.prepare(
				`select id from data where upper >= $gte and lower <= $lte`
			)
			for (const [gte, lte] of ranges) overlapsQuery.all({ gte, lte })
		}
	)

	await bench.warmup()
	await bench.run()

	printTable(bench)
}

async function test3() {
	// Lets try something a little more grounded.
	let tree: AsyncIntervalTree<[string, string, string], number>
	let db: Database

	const startMs = Date.now()
	const durationMs = 20 * DecadeMs
	var { items, ranges } = createCalendarData(startMs, durationMs)

	// Add a parasitic event that spans the entire length.
	{
		const min = new Date(startMs).toISOString()
		const max = new Date(startMs + durationMs).toISOString()
		items.push({ key: [min, max, randomId()], value: -1 })
	}

	{
		const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
		tree = new AsyncIntervalTree(
			storage,
			1,
			40,
			jsonCodec.compare,
			jsonCodec.compare
		)
		await tree.write({ set: items })
	}

	{
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

		console.log("HERE", db.prepare("PRAGMA compile_options").all())
		// SQLITE_ENABLE_RTREE
		throw new Error("HERE")
		writeQuery({ set: items })
	}

	let sampled: typeof ranges
	const bench = new Bench({
		time: 2000,
		iterations: 10,
		setup: async () => {
			console.log("sample")
			sampled = Array(1000)
				.fill(0)
				.map(() => sample(ranges)!)
		},
	})

	bench.add(`parasitic calendar interval tree on leveldb`, async () => {
		for (const [gte, lte] of sampled) await tree.overlaps({ gte, lte })
	})

	bench.add(`parasitic calendar sqlite overlaps`, async () => {
		const overlapsQuery = db.prepare(
			`select id from data where upper >= $gte and lower <= $lte`
		)
		for (const [gte, lte] of sampled) overlapsQuery.all({ gte, lte })
	})

	await bench.warmup()
	await bench.run()

	printTable(bench)
}

// npm uninstall better-sqlite3
// sh build-sqlite.sh
async function test4() {
	let tree: AsyncIntervalTree<[string, string, string], number>
	let db: Database

	const startMs = Date.now()
	const durationMs = 20 * DecadeMs
	var { items, ranges } = createCalendarData(startMs, durationMs)

	// Add a parasitic event that spans the entire length.
	{
		const min = new Date(startMs).toISOString()
		const max = new Date(startMs + durationMs).toISOString()
		items.push({ key: [min, max, randomId()], value: -1 })
	}

	{
		const storage = new LevelDbKeyValueStorage(new Level(tmp("data.leveldb")))
		tree = new AsyncIntervalTree(
			storage,
			1,
			40,
			jsonCodec.compare,
			jsonCodec.compare
		)
		await tree.write({ set: items })
	}

	{
		// CREATE VIRTUAL TABLE <name> USING rtree(<column-names>);

		db = sqlite(tmp("data.sqlite"))
		db.prepare(
			`create table data ( id text primary key, lower int, upper int, value int)`
		).run()

		db.prepare(
			`create virtual table data_ranges using rtree ( id, lower, upper )`
		).run()

		const insertQuery = db.prepare(
			`insert or replace into data values ($id, strftime('%s', $lower), strftime('%s', $upper), $value)`
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

		// console.log(db.prepare("PRAGMA compile_options").all())

		writeQuery({ set: items })
	}

	let sampled: typeof ranges
	const bench = new Bench({
		time: 2000,
		iterations: 10,
		setup: async () => {
			console.log("sample")
			sampled = Array(1000)
				.fill(0)
				.map(() => sample(ranges)!)
		},
	})

	bench.add(`parasitic calendar interval tree on leveldb`, async () => {
		for (const [gte, lte] of sampled) await tree.overlaps({ gte, lte })
	})

	bench.add(`parasitic calendar sqlite rtree overlaps`, async () => {
		try {
			const overlapsQuery = db.prepare(
				`
				select id from data_ranges
				where upper >= $gte
				and lower <= $lte
				`
			)
			for (const [gte, lte] of sampled) overlapsQuery.all({ gte, lte })
		} catch (e) {
			console.error(e)
		}
	})

	await bench.warmup()
	await bench.run()

	printTable(bench)
}

const SecondMs = 1000
const MinuteMs = SecondMs * 60
const HourMs = MinuteMs * 60
const DayMs = HourMs * 24
const YearMs = DayMs * 365
const DecadeMs = YearMs * 10

function createCalendarData(startMs: number, durationMs: number) {
	let ranges: [string, string][] = []
	const items: { key: [string, string, string]; value: number }[] = []

	// 5-15x 15min-3hr long meetings / week
	const WeekMs = DayMs * 7
	for (let i = 0; i < durationMs; i += WeekMs) {
		for (let j = 0; j < randomInt([5, 15]); j++) {
			const duration = randomInt([HourMs / 4, HourMs * 3])
			const start = i + randomInt([0, WeekMs])
			const min = new Date(startMs + start).toISOString()
			const max = new Date(startMs + start + duration).toISOString()
			items.push({ key: [min, max, randomId()], value: i * j })
		}
	}

	// 3-12x 1day-4day events per month.
	const MonthMs = DayMs * 30
	for (let i = 0; i < durationMs; i += MonthMs) {
		for (let j = 0; j < randomInt([5, 15]); j++) {
			const duration = randomInt([DayMs, DayMs * 4])
			const start = i + randomInt([0, MonthMs])
			const min = new Date(startMs + start).toISOString()
			const max = new Date(startMs + start + duration).toISOString()
			items.push({ key: [min, max, randomId()], value: i * j })
		}
	}

	// 2-5x 5day-20day events per year.
	for (let i = 0; i < durationMs; i += YearMs) {
		for (let j = 0; j < randomInt([2, 5]); j++) {
			const duration = randomInt([DayMs * 5, DayMs * 20])
			const start = i + randomInt([0, YearMs])
			const min = new Date(startMs + start).toISOString()
			const max = new Date(startMs + start + duration).toISOString()
			items.push({ key: [min, max, randomId()], value: i * j })
		}
	}

	// Query for every month in the decade
	for (let i = 0; i < durationMs; i += MonthMs) {
		// Every day view
		for (let j = 0; j < MonthMs; j += DayMs) {
			const min = new Date(startMs + i + j).toISOString()
			const max = new Date(startMs + i + j + DayMs).toISOString()
			ranges.push([min, max])
		}

		for (let j = 0; j < MonthMs; j += WeekMs) {
			// Every view
			const min = new Date(startMs + i + j).toISOString()
			const max = new Date(startMs + i + j + WeekMs).toISOString()
			ranges.push([min, max])
		}

		{
			// Month view
			const min = new Date(startMs + i).toISOString()
			const max = new Date(startMs + i + WeekMs).toISOString()
			ranges.push([min, max])
		}
	}

	console.log("items", items.length)
	console.log("ranges", ranges.length)
	return { items, ranges }
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}

function randomNumber(range: [number, number]) {
	return Math.random() * (range[1] - range[0]) + range[0]
}

function randomInt(range: [number, number]) {
	return Math.round(randomNumber(range))
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

test4()
