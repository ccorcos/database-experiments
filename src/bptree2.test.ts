import { strict as assert } from "assert"
import { jsonCodec } from "lexicodec"
import { sum, uniqWith } from "lodash"
import { describe, it } from "mocha"
import { BinaryPlusTree2 } from "./bptree2"

// min = 2, max = 4
const structuralTests24 = `
+ 5
[5]

+ 10
[5,10]

+ 3
[3,5,10]

// Delete from root leaf
- 5
[3,10]

+ 5
[3,5,10]

+ 7
[3,5,7,10]

// Split
+ 6
[null,7]
[3,5,6] [7,10]

// Merge right branch
- 7
[3,5,6,10]

+ 7
[null,7]
[3,5,6] [7,10]

- 6
[null,7]
[3,5] [7,10]

// Merge left branch
- 5
[3,7,10]

+ 5
[3,5,7,10]

+ 6
[null,7]
[3,5,6] [7,10]

+ 14
[null,7]
[3,5,6] [7,10,14]

+ 23
[null,7]
[3,5,6] [7,10,14,23]

+ 24
[null,7,23]
[3,5,6] [7,10,14] [23,24]

// Merge right branch
- 23
[null,7]
[3,5,6] [7,10,14,24]

+ 23
[null,7,23]
[3,5,6] [7,10,14] [23,24]

// Update parent minKey
- 7
[null,10,23]
[3,5,6] [10,14] [23,24]

// Merge middle branch
- 14
[null,23]
[3,5,6,10] [23,24]

+ 14
[null,10,23]
[3,5,6] [10,14] [23,24]

- 3
[null,10,23]
[5,6] [10,14] [23,24]

// Merge left branch
- 6
[null,23]
[5,10,14] [23,24]

+ 3
[null,23]
[3,5,10,14] [23,24]

+ 6
[null,10,23]
[3,5,6] [10,14] [23,24]

+ 7
[null,10,23]
[3,5,6,7] [10,14] [23,24]

+ 8
[null,7,10,23]
[3,5,6] [7,8] [10,14] [23,24]

+ 11
[null,7,10,23]
[3,5,6] [7,8] [10,11,14] [23,24]

+ 12
[null,7,10,23]
[3,5,6] [7,8] [10,11,12,14] [23,24]

// Double split
+ 13
[null,13]
[null,7,10] [13,23]
[3,5,6] [7,8] [10,11,12] [13,14] [23,24]

+ 15
[null,13]
[null,7,10] [13,23]
[3,5,6] [7,8] [10,11,12] [13,14,15] [23,24]

// Double update minKey
- 13
[null,14]
[null,7,10] [14,23]
[3,5,6] [7,8] [10,11,12] [14,15] [23,24]

// Double merge mid-right branch
- 14
[null,7,10,15]
[3,5,6] [7,8] [10,11,12] [15,23,24]

+ 2
[null,7,10,15]
[2,3,5,6] [7,8] [10,11,12] [15,23,24]

+ 4
[null,10]
[null,5,7] [10,15]
[2,3,4] [5,6] [7,8] [10,11,12] [15,23,24]

- 8
[null,10]
[null,5] [10,15]
[2,3,4] [5,6,7] [10,11,12] [15,23,24]

- 3
[null,10]
[null,5] [10,15]
[2,4] [5,6,7] [10,11,12] [15,23,24]

// Double merge left branch
- 2
[null,10,15]
[4,5,6,7] [10,11,12] [15,23,24]

- 15
[null,10,23]
[4,5,6,7] [10,11,12] [23,24]

+ 20
[null,10,23]
[4,5,6,7] [10,11,12,20] [23,24]

// Redistribute right
- 24
[null,10,20]
[4,5,6,7] [10,11,12] [20,23]

+ 13
[null,10,20]
[4,5,6,7] [10,11,12,13] [20,23]

- 4
[null,10,20]
[5,6,7] [10,11,12,13] [20,23]

- 5
[null,10,20]
[6,7] [10,11,12,13] [20,23]

// Redistribute left
- 6
[null,12,20]
[7,10,11] [12,13] [20,23]

`

describe("BinaryPlusTree2", () => {
	describe("structural tests 2-4", () => {
		const tree = new BinaryPlusTree2(2, 4)
		test(tree, structuralTests24)
	})

	describe("property test 2-4 * 100", () => {
		propertyTest({ minSize: 2, maxSize: 4, testSize: 100 })
	})

	describe("property test 3-6 * 100", () => {
		propertyTest({ minSize: 3, maxSize: 6, testSize: 100 })
	})

	it("big tree", () => {
		const numbers = randomNumbers(20_000)
		const tree = new BinaryPlusTree2(3, 9)
		for (const number of numbers) {
			tree.set(number, number * 2)
			assert.equal(tree.get(number), number * 2)
		}
		for (const number of numbers) {
			tree.delete(number)
			assert.equal(tree.get(number), undefined)
		}
		assert.equal(tree.depth(), 1)
	})

	it("tuple keys", () => {
		const tree = new BinaryPlusTree2<any[], any>(3, 9, jsonCodec.compare)

		const numbers = randomNumbers(2000)
		for (const number of numbers) {
			tree.set(["user", number], { id: number })
			tree.set(["profile", number], number)
			assert.deepEqual(tree.get(["user", number]), { id: number })
			assert.deepEqual(tree.get(["profile", number]), number)
		}

		for (const number of numbers) {
			tree.delete(["user", number])
			assert.equal(tree.get(["user", number]), undefined)
		}
	})

	it("list", () => {
		const numbers = Array(1000)
			.fill(0)
			.map((x, i) => i * 2)
		const tree = new BinaryPlusTree2(3, 9)
		for (const number of numbers) {
			tree.set(number, number)
		}

		// Entire thing
		assert.deepEqual(
			tree.list({}),
			numbers.map((n) => ({ key: n, value: n }))
		)

		// No start bound
		assert.deepEqual(tree.list({ end: 9 }), [
			{ key: 0, value: 0 },
			{ key: 2, value: 2 },
			{ key: 4, value: 4 },
			{ key: 6, value: 6 },
			{ key: 8, value: 8 },
		])

		// Within the same branch.
		assert.deepEqual(tree.list({ start: 3, end: 9 }), [
			{ key: 4, value: 4 },
			{ key: 6, value: 6 },
			{ key: 8, value: 8 },
		])

		assert.deepEqual(tree.list({ start: 4, end: 10 }), [
			{ key: 4, value: 4 },
			{ key: 6, value: 6 },
			{ key: 8, value: 8 },
		])

		// Across branches.
		assert.deepEqual(tree.list({ start: 4, end: 24 }), [
			{ key: 4, value: 4 },
			{ key: 6, value: 6 },
			{ key: 8, value: 8 },
			{ key: 10, value: 10 },
			{ key: 12, value: 12 },
			{ key: 14, value: 14 },
			{ key: 16, value: 16 },
			{ key: 18, value: 18 },
			{ key: 20, value: 20 },
			{ key: 22, value: 22 },
		])

		// No end bound.
		assert.deepEqual(tree.list({ start: 2000 - 4 }), [
			{ key: 1996, value: 1996 },
			{ key: 1998, value: 1998 },
		])

		// Limit.
		assert.deepEqual(tree.list({ start: 4, end: 24, limit: 4 }), [
			{ key: 4, value: 4 },
			{ key: 6, value: 6 },
			{ key: 8, value: 8 },
			{ key: 10, value: 10 },
		])
	})

	it("list property test", () => {
		const randomTuples = (
			n: number,
			len: number,
			range: [number, number] = [-10, 10]
		) =>
			Array(n)
				.fill(0)
				.map(() => randomNumbers(len, range))

		let tuples = [
			...randomTuples(10, 1),
			...randomTuples(50, 2),
			...randomTuples(100, 3),
			...randomTuples(500, 4),
			...randomTuples(1000, 5),
		]

		tuples = uniqWith(tuples, (a, b) => jsonCodec.compare(a, b) === 0)
		tuples.sort(jsonCodec.compare)

		const tree = new BinaryPlusTree2(3, 9, jsonCodec.compare)
		for (const tuple of tuples) {
			tree.set(tuple, sum(tuple))
		}

		const ranges = randomTuples(10_000, 2, [0, tuples.length - 1])
			.map((range) => {
				range.sort(jsonCodec.compare)
				return range
			})
			// Ignore ranges where start and end are the same.
			.filter(([a, b]) => a !== b)

		for (const tuple of tuples) {
			const result = tree.get(tuple)
			assert.deepEqual(result, sum(tuple))
		}

		for (const range of ranges) {
			const start = tuples[range[0]]
			const end = tuples[range[1]]
			const result = tree.list({ start, end }).map(({ key }) => key)
			const target = tuples.slice(range[0], range[1])
			assert.deepEqual(
				result,
				target,
				`range: [${range[0]},	${range[1]}] start: ${JSON.stringify(
					start
				)} end: ${JSON.stringify(end)}`
			)
		}
	})

	function propertyTest(args: {
		minSize: number
		maxSize: number
		testSize: number
	}) {
		const size = args.testSize
		const numbers = randomNumbers(size)

		const tree = new BinaryPlusTree2(args.minSize, args.maxSize)
		for (let i = 0; i < size; i++) {
			const n = numbers[i]
			it(`Set ${i} : ${n}`, () => {
				// it(`+ ${n}`, () => {
				tree.set(n, n.toString())
				verify(tree)

				// Get works on every key so far.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]
					assert.equal(tree.get(x), x.toString())
				}
				// })

				// Overwrite the jth key.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]

					// it(`Overwrite ${j}: ${x}`, () => {
					const t = tree.clone()
					t.set(x, x * 2)
					verify(t)

					// Check get on all keys.
					for (let k = 0; k <= i; k++) {
						const y = numbers[k]
						if (x === y) assert.equal(t.get(y), y * 2)
						else assert.equal(t.get(y), y.toString())
					}
					// })
				}

				// Delete the jth key.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]

					// it(`Delete ${j} : ${x}`, () => {
					const t = tree.clone()
					t.delete(x)
					try {
						verify(t)
					} catch (error) {
						console.log("BEFORE", inspect(tree))
						console.log("DELETE", x)
						console.log("AFTER", inspect(t))
						throw error
					}

					// Check get on all keys.
					for (let k = 0; k <= i; k++) {
						const y = numbers[k]
						if (x === y) assert.equal(t.get(y), undefined)
						else assert.equal(t.get(y), y.toString())
					}
					// })
				}
			})
		}
	}
})

function randomNumbers(size: number, range?: [number, number]) {
	if (!range) range = [-size * 10, size * 10]
	const numbers: number[] = []
	for (let i = 0; i < size; i++)
		numbers.push(Math.round(Math.random() * (range[1] - range[0]) - range[0]))
	return numbers
}

function parseTests(str: string) {
	// Cleanup extra whitespace
	str = str
		.split("\n")
		.map((line) => line.trim())
		.join("\n")
		.trim()

	return str.split("\n\n").map((block) => {
		const lines = block.split("\n")
		let comment = ""
		if (lines[0].startsWith("//")) {
			comment = lines[0].slice(3)
			lines.splice(0, 1)
		}
		const [op, nStr] = lines[0].split(" ")
		const n = parseInt(nStr)
		const tree = lines.slice(1).join("\n")
		return { comment, n, tree, op: op as "+" | "-" }
	})
}

function test(tree: BinaryPlusTree2, str: string) {
	for (const test of parseTests(structuralTests24)) {
		let label = `${test.op} ${test.n}`
		if (test.comment) label += " // " + test.comment
		it(label, () => {
			if (test.op === "+") tree.set(test.n, test.n.toString())
			if (test.op === "-") tree.delete(test.n)
			assert.equal(inspect(tree), test.tree, test.comment)

			const value = test.op === "+" ? test.n.toString() : undefined
			assert.equal(tree.get(test.n), value, test.comment)

			assert.equal(tree.depth(), test.tree.split("\n").length, test.comment)

			verify(tree)
		})
	}
}

type Key = string | number | null
type KeyTree =
	| { keys: Key[]; children?: undefined }
	| { keys: Key[]; children: KeyTree[] }

function toKeyTree(tree: BinaryPlusTree2, id = "root"): KeyTree {
	const node = tree.nodes[id]
	if (!node) throw new Error("Missing node!")

	const keys = node.leaf
		? node.values.map((v) => v.key)
		: node.children.map((v) => v.minKey)

	if (node.leaf) return { keys: keys }
	const subtrees = node.children.map((v) => toKeyTree(tree, v.childId))

	return { keys: keys, children: subtrees }
}

type TreeLayer = Key[][]

function toTreeLayers(tree: KeyTree): TreeLayer[] {
	const layers: TreeLayer[] = []

	let cursor = [tree]
	while (cursor.length > 0) {
		const layer: TreeLayer = []
		const nextCursor: KeyTree[] = []
		for (const tree of cursor) {
			layer.push(tree.keys)
			if (tree.children) nextCursor.push(...tree.children)
		}
		layers.push(layer)
		cursor = nextCursor
	}
	return layers
}

function print(x: any) {
	if (x === null) return "null"
	if (typeof x === "number") return x.toString()
	if (typeof x === "string") return JSON.stringify(x)
	if (Array.isArray(x)) return "[" + x.map(print).join(",") + "]"
	return ""
}

function inspect(tree: BinaryPlusTree2) {
	const keyTree = toKeyTree(tree)
	const layers = toTreeLayers(keyTree)
	const str = layers
		.map((layer) =>
			layer.length === 1 ? print(layer[0]) : layer.map(print).join(" ")
		)
		.join("\n")
	return str
}

/** Check for node sizes. */
function verify(tree: BinaryPlusTree2, id = "root") {
	const node = tree.nodes[id]
	if (id === "root") {
		assert.equal(countNodes(tree), Object.keys(tree.nodes).length)
		if (!node) return
		if (node.leaf) return
		for (const { childId } of node.children) verify(tree, childId)
		return
	}

	assert.ok(node)
	const size = node.leaf ? node.values.length : node.children.length
	assert.ok(size >= tree.minSize)
	assert.ok(size <= tree.maxSize, inspect(tree))

	if (node.leaf) return
	for (const { childId } of node.children) verify(tree, childId)
}

function countNodes(tree: BinaryPlusTree2, id = "root") {
	const node = tree.nodes[id]
	if (id === "root") {
		if (!node) return 0
		if (node.leaf) return 1
		let count = 1
		for (const { childId } of node.children) count += countNodes(tree, childId)
		return count
	}

	assert.ok(node)
	if (node.leaf) return 1
	let count = 1
	for (const { childId } of node.children) count += countNodes(tree, childId)
	return count
}
