import { strict as assert } from "assert"
import { cloneDeep, isEqual, uniq } from "lodash"
import { describe, it } from "mocha"
import { BinaryPlusKeyValueDatabase } from "./bptree-kv"
import { KeyValueDatabase } from "./kv"

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

// Skipping becuase this is pretty slow.
describe.skip("BinaryPlusKeyValueDatabase", () => {
	describe("structural tests 2-4", () => {
		const kv = new KeyValueDatabase()
		const tree = new BinaryPlusKeyValueDatabase(kv, 2, 4)
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
		const kv = new KeyValueDatabase()
		const tree = new BinaryPlusKeyValueDatabase(kv, 3, 9)
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

	function propertyTest(args: {
		minSize: number
		maxSize: number
		testSize: number
	}) {
		const numbers = randomNumbers(args.testSize)

		const kv = new KeyValueDatabase()
		const tree = new BinaryPlusKeyValueDatabase(kv, args.minSize, args.maxSize)
		for (let i = 0; i < numbers.length; i++) {
			const n = numbers[i]
			it(`Set ${i} : ${n}`, () => {
				// it(`+ ${n}`, () => {

				verifyImmutable(tree, () => {
					tree.set(n, n.toString())
					verify(tree)
				})

				for (let j = 0; j <= i; j++) {
					const x = numbers[j]
					assert.equal(tree.get(x), x.toString())
				}
				// })

				// Overwrite the jth key.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]

					// it(`Overwrite ${j}: ${x}`, () => {
					const t = clone(tree)

					verifyImmutable(tree, () => {
						t.set(x, x * 2)
						verify(t)
					})

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
					const t = clone(tree)
					verifyImmutable(tree, () => {
						t.delete(x)
						verify(t)
					})

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

function randomNumbers(size: number) {
	const numbers: number[] = []
	for (let i = 0; i < size; i++)
		numbers.push(Math.round((Math.random() - 0.5) * size * 10))
	return uniq(numbers)
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

function test(tree: BinaryPlusKeyValueDatabase, str: string) {
	for (const test of parseTests(str)) {
		let label = `${test.op} ${test.n}`
		if (test.comment) label += " // " + test.comment
		it(label, () => {
			if (test.op === "+") tree.set(test.n, test.n.toString())
			if (test.op === "-") tree.delete(test.n)
			assert.equal(inspect(tree), test.tree, test.comment)

			const value = test.op === "+" ? test.n.toString() : undefined
			assert.equal(tree.get(test.n), value, test.comment)

			assert.equal(tree.depth(), test.tree.split("\n").length, test.comment)
		})
	}
}

type Key = string | number
type KeyTree =
	| { keys: Key[]; children?: undefined }
	| { keys: Key[]; children: KeyTree[] }

function toKeyTree(tree: BinaryPlusKeyValueDatabase, id = "root"): KeyTree {
	const node = tree.kv.get(id)?.value
	if (!node) {
		console.warn("Missing node!")
		// throw new Error("Missing node!")
		return { keys: [] }
	}

	const keys = node.values.map((v) => v.key)
	if (node.leaf) return { keys: keys }

	const subtrees = node.values.map((v) => toKeyTree(tree, v.value))
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

function inspect(tree: BinaryPlusKeyValueDatabase) {
	const keyTree = toKeyTree(tree)
	const layers = toTreeLayers(keyTree)
	const str = layers
		.map((layer) =>
			layer.length === 1 ? print(layer[0]) : layer.map(print).join(" ")
		)
		.join("\n")
	return str
}

function clone(tree: BinaryPlusKeyValueDatabase) {
	const kv = new KeyValueDatabase()
	kv.map = cloneDeep(tree.kv.map)
	const cloned = new BinaryPlusKeyValueDatabase(kv, tree.minSize, tree.maxSize)
	return cloned
}

function shallowClone(tree: BinaryPlusKeyValueDatabase) {
	const kv = new KeyValueDatabase()
	kv.map = { ...tree.kv.map }
	const cloned = new BinaryPlusKeyValueDatabase(kv, tree.minSize, tree.maxSize)
	return cloned
}

/** Check for node sizes. */
function verify(tree: BinaryPlusKeyValueDatabase, id = "root") {
	const node = tree.kv.get(id)?.value
	if (id === "root") {
		assert.equal(countNodes(tree), Object.keys(tree.kv.map).length)
		if (!node) return
		if (node.leaf) return
		for (const { value } of node.values) verify(tree, value)
		return
	}

	assert.ok(node)
	assert.ok(node.values.length >= tree.minSize)
	assert.ok(node.values.length <= tree.maxSize, inspect(tree))

	if (node.leaf) return
	for (const { value } of node.values) verify(tree, value)
}

function countNodes(tree: BinaryPlusKeyValueDatabase, id = "root") {
	const node = tree.kv.get(id)?.value
	if (id === "root") {
		if (!node) return 0
		if (node.leaf) return 1
		let count = 1
		for (const { value } of node.values) count += countNodes(tree, value)
		return count
	}

	assert.ok(node)
	assert.ok(node.values.length >= tree.minSize)
	assert.ok(node.values.length <= tree.maxSize, inspect(tree))

	if (node.leaf) return 1
	let count = 1
	for (const { value } of node.values) count += countNodes(tree, value)
	return count
}

function verifyImmutable(tree: BinaryPlusKeyValueDatabase, fn: () => void) {
	const shallow = shallowClone(tree)
	const deep = clone(tree)

	fn()

	const keys = uniq([...Object.keys(tree.kv.map), ...Object.keys(shallow)])
	for (const key of keys) {
		const newNode = tree.kv.get(key)
		const originalValue = deep.kv.get(key)
		const originalRef = shallow.kv.get(key)

		if (isEqual(newNode, originalValue)) {
			assert.ok(
				newNode === originalRef
				// [inspect(deep), inspect(tree), JSON.stringify(newNode)].join("\n\n")
			)
		} else {
			assert.ok(
				newNode !== originalRef
				// [inspect(deep), inspect(tree), JSON.stringify(newNode)].join("\n\n")
			)
		}
	}
}
