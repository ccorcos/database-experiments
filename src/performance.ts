import { insert, remove } from "@ccorcos/ordered-array"
import { sampleSize, shuffle } from "lodash"
import { Bench } from "tinybench"
import { BinaryPlusTree2 } from "./bptree2"

// bptree with sqlite vs bptree with leveldb vs sqlite vs leveldb
// sqlite vs tuple bptree
// sqlite vs reducer tree
// sqlite vs interval tree

function prettyNs(timeNs: number) {
	const round = (n: number) =>
		(Math.round(n * 1000) / 1000).toFixed(3).padStart(7, "0")

	const seconds = timeNs / (1000 * 1000 * 1000)
	if (seconds >= 1) return round(seconds) + "s"

	const ms = timeNs / (1000 * 1000)
	if (ms >= 1) return round(ms) + "ms"

	const us = timeNs / 1000
	if (us >= 1) return round(us) + "μs"

	return round(timeNs) + "ns"
}

function printTable(bench: Bench) {
	const data = bench.table()
	console.table(
		data.map((item) => {
			if (!item) return
			const { "Average Time (ns)": time, ...rest } = item
			return {
				"Average Time": prettyNs(time as number),
				...rest,
			}
		})
	)
}

async function test1() {
	const numbers = shuffle(
		Array(10_000)
			.fill(0)
			.map((x, i) => i)
	)
	const bench = new Bench({ time: 2000, iterations: 2 })
	bench
		.add("insert 10_000 ordered array", () => {
			console.log("insert 10_000 ordered array")
			const list: number[] = []
			for (const n of numbers) insert(list, n)
		})

		.add("insert 10_000 bptree2 50-100", async () => {
			console.log("insert 10_000 bptree2 50-100")
			const tree = new BinaryPlusTree2(50, 100)
			for (const n of numbers) tree.set(n, null)
		})

	await bench.warmup()
	await bench.run()

	printTable(bench)
	// ┌─────────┬──────────────┬────────────────────────────────┬─────────┬──────────┬─────────┐
	// │ (index) │ Average Time │ Task Name                      │ ops/sec │ Margin   │ Samples │
	// ├─────────┼──────────────┼────────────────────────────────┼─────────┼──────────┼─────────┤
	// │ 0       │ '004.559ms'  │ 'insert 10_000 ordered array'  │ '219'   │ '±0.36%' │ 439     │
	// │ 1       │ '004.037ms'  │ 'insert 10_000 bptree2 50-100' │ '247'   │ '±1.67%' │ 496     │
	// └─────────┴──────────────┴────────────────────────────────┴─────────┴──────────┴─────────┘
}

async function test2() {
	const numbers = shuffle(
		Array(100_000)
			.fill(0)
			.map((x, i) => i)
	)
	const bench = new Bench({ time: 2000, iterations: 2 })
	bench
		.add("insert 100_000 ordered array", () => {
			console.log("insert 100_000 ordered array")
			const list: number[] = []
			for (const n of numbers) insert(list, n)
		})
		.add("insert 100_000 bptree2 10-20", async () => {
			console.log("insert 100_000 bptree2 10-20")
			const tree = new BinaryPlusTree2(10, 20)
			for (const n of numbers) tree.set(n, null)
		})
		.add("insert 100_000 bptree2 50-100", async () => {
			console.log("insert 100_000 bptree2 50-100")
			const tree = new BinaryPlusTree2(50, 100)
			for (const n of numbers) tree.set(n, null)
		})
		.add("insert 100_000 bptree2 100-200", async () => {
			console.log("insert 100_000 bptree2 100-200")
			const tree = new BinaryPlusTree2(100, 200)
			for (const n of numbers) tree.set(n, null)
		})

	await bench.warmup()
	await bench.run()

	printTable(bench)

	// ┌─────────┬──────────────┬──────────────────────────────────┬─────────┬──────────┬─────────┐
	// │ (index) │ Average Time │ Task Name                        │ ops/sec │ Margin   │ Samples │
	// ├─────────┼──────────────┼──────────────────────────────────┼─────────┼──────────┼─────────┤
	// │ 0       │ '401.652ms'  │ 'insert 100_000 ordered array'   │ '2'     │ '±3.47%' │ 5       │
	// │ 1       │ '070.908ms'  │ 'insert 100_000 bptree2 10-20'   │ '14'    │ '±2.89%' │ 29      │
	// │ 2       │ '052.286ms'  │ 'insert 100_000 bptree2 50-100'  │ '19'    │ '±2.06%' │ 39      │
	// │ 3       │ '051.514ms'  │ 'insert 100_000 bptree2 100-200' │ '19'    │ '±1.85%' │ 39      │
	// └─────────┴──────────────┴──────────────────────────────────┴─────────┴──────────┴─────────┘
}

async function test3() {
	const baseArray = shuffle(
		Array(100_000)
			.fill(0)
			.map((x, i) => i)
	)

	const baseTree = new BinaryPlusTree2(50, 100)
	for (const n of baseArray) baseTree.set(n, null)

	let array: number[] = []
	let tree = new BinaryPlusTree2(50, 100)

	const bench = new Bench({
		time: 2000,
		iterations: 2,
		setup: () => {
			array = [...baseArray]
			tree = baseTree.clone()
		},
	})

	const insertNumbers = sampleSize(baseArray, 1000).map((n) => n + 0.5)
	const deleteNumbers = sampleSize(baseArray, 1000)

	bench
		.add("insert 1000 more from array 100k", () => {
			for (const n of insertNumbers) insert(array, n)
		})
		.add("insert 1000 more bptree2 50-100 100k", async () => {
			for (const n of insertNumbers) tree.set(n, null)
		})
		.add("delete 1000 more from array 100k", async () => {
			for (const n of deleteNumbers) remove(array, n)
		})
		.add("delete 1000 more bptree2 50-100 100k", async () => {
			for (const n of deleteNumbers) tree.delete(n)
		})

	await bench.warmup()
	await bench.run()

	printTable(bench)

	// ┌─────────┬──────────────┬────────────────────────────────────────┬─────────┬───────────┬─────────┐
	// │ (index) │ Average Time │ Task Name                              │ ops/sec │ Margin    │ Samples │
	// ├─────────┼──────────────┼────────────────────────────────────────┼─────────┼───────────┼─────────┤
	// │ 0       │ '012.094ms'  │ 'insert 1000 more from array 100k'     │ '82'    │ '±23.60%' │ 167     │
	// │ 1       │ '557.679μs'  │ 'insert 1000 more bptree2 50-100 100k' │ '1,793' │ '±0.52%'  │ 3587    │
	// │ 2       │ '169.889μs'  │ 'delete 1000 more from array 100k'     │ '5,886' │ '±0.22%'  │ 11773   │
	// │ 3       │ '547.311μs'  │ 'delete 1000 more bptree2 50-100 100k' │ '1,827' │ '±0.33%'  │ 3655    │
	// └─────────┴──────────────┴────────────────────────────────────────┴─────────┴───────────┴─────────┘
}

test3()
