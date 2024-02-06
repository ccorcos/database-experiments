import { TestClock } from "@ccorcos/test-clock"
import { strict as assert } from "assert"
import { cloneDeep, isEqual, uniq } from "lodash"
import { describe, it } from "mocha"
import {
	AsyncBinaryPlusTree,
	AsyncKeyValueDatabase,
	AsyncKeyValueTransaction,
} from "./AsyncBinaryPlusTree"

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

// Test class to make assertions about locks.
// Also introduce delays for concurrency.
class TestAsyncKeyValueDatabase<T> extends AsyncKeyValueDatabase<T> {
	constructor(private delay?: () => Promise<void>) {
		super()
	}

	async get(key: string) {
		await this.delay?.()
		return this.map.get(key)
	}

	async write(tx: { set?: { key: string; value: T }[]; delete?: string[] }) {
		await this.delay?.()
		for (const { key, value } of tx.set || []) this.map.set(key, value)
		for (const key of tx.delete || []) this.map.delete(key)
	}

	transact() {
		return new TestTransaction<T>(this)
	}
}

class TestTransaction<T> extends AsyncKeyValueTransaction<T> {
	readLocks = new Set<string>()
	async readLock(key: string) {
		const release = await super.readLock(key)

		assert.equal(this.readLocks.has(key), false)
		assert.equal(this.writeLocks.has(key), false)

		this.readLocks.add(key)
		return () => {
			this.readLocks.delete(key)
			release()
		}
	}

	writeLocks = new Set<string>()
	async writeLock(key: string) {
		const release = await super.writeLock(key)

		assert.equal(this.readLocks.has(key), false)
		assert.equal(this.writeLocks.has(key), false)

		this.writeLocks.add(key)
		// console.trace("Lock", key)
		return () => {
			// console.trace("Release", key)
			this.writeLocks.delete(key)
			release()
		}
	}

	async get(key: string): Promise<T | undefined> {
		assert.equal(this.readLocks.has(key) || this.writeLocks.has(key), true)
		return super.get(key)
	}

	set(key: string, value: T) {
		return super.set(key, value)
	}

	delete(key: string) {
		assert.equal(this.writeLocks.has(key), true)
		return super.delete(key)
	}

	async commit() {
		for (const key in this.sets) {
			if ((await this.kv.get(key)) !== undefined) {
				assert.equal(
					this.writeLocks.has(key),
					true,
					"Missing write lock for " + key
				)
			}
		}
		for (const key of this.deletes) {
			assert.equal(
				this.writeLocks.has(key),
				true,
				"Missing delete lock for " + key
			)
		}
		return super.commit()
	}
}

// Skipping becuase this is pretty slow.
describe("AsyncBinaryPlusTree", function () {
	this.timeout(10_000)

	describe("structural tests 2-4", async () => {
		const kv = new TestAsyncKeyValueDatabase() as AsyncKeyValueDatabase
		const tree = new AsyncBinaryPlusTree(kv, 2, 4)
		await test(tree, structuralTests24)
	})

	describe("property test 2-4 * 100", async () => {
		await propertyTest({ minSize: 2, maxSize: 4, testSize: 100 })
	})

	describe("property test 3-6 * 100", async () => {
		await propertyTest({ minSize: 3, maxSize: 6, testSize: 100 })
	})

	it("big tree", async () => {
		const numbers = randomNumbers(20_000)
		const kv = new TestAsyncKeyValueDatabase() as AsyncKeyValueDatabase
		const tree = new AsyncBinaryPlusTree(kv, 3, 9)
		for (const number of numbers) {
			await tree.set(number, number * 2)
			assert.equal(await tree.get(number), number * 2)
		}
		for (const number of numbers) {
			await tree.delete(number)
			assert.equal(await tree.get(number), undefined)
		}
		assert.equal(await tree.depth(), 1)
	})

	it("concurreny reads and write", async () => {
		const clock = new TestClock()

		const sleep = (n: number) => clock.sleep(Math.random() * n)

		const kv = new TestAsyncKeyValueDatabase(() =>
			sleep(5)
		) as AsyncKeyValueDatabase
		const tree = new AsyncBinaryPlusTree(kv, 3, 6)

		const size = 5000
		const numbers = randomNumbers(size)

		const writeAll = () =>
			numbers.map(async (number) => {
				await sleep(20)
				await tree.set(number, number)
			})

		const readAll = () =>
			numbers.map(async (number) => {
				await sleep(20)
				await tree.get(number)
			})

		const deleteSome = (modN: number) =>
			numbers.map(async (number, index) => {
				if (index % modN !== 0) return
				await sleep(20)
				await tree.delete(number)
			})

		const promises = [
			writeAll(),
			writeAll(),
			writeAll(),
			readAll(),
			readAll(),
			readAll(),
			deleteSome(7),
			deleteSome(5),
			deleteSome(5),
		]

		await clock.run()
		await Promise.all(promises)

		await Promise.all(
			numbers.map(async (number, index) => {
				if (index % 7 === 0 || index % 5 === 0) return
				const result = await tree.get(number)
				assert.equal(result, number)
			})
		)
	})

	async function propertyTest(args: {
		minSize: number
		maxSize: number
		testSize: number
	}) {
		const numbers = randomNumbers(args.testSize)

		const kv = new TestAsyncKeyValueDatabase() as AsyncKeyValueDatabase
		const tree = new AsyncBinaryPlusTree(kv, args.minSize, args.maxSize)
		for (let i = 0; i < numbers.length; i++) {
			const n = numbers[i]
			it(`Set ${i} : ${n}`, async () => {
				// it(`+ ${n}`, () => {

				await verifyImmutable(tree, async () => {
					await tree.set(n, n.toString())
					await verify(tree)
				})

				for (let j = 0; j <= i; j++) {
					const x = numbers[j]
					assert.equal(await tree.get(x), x.toString())
				}
				// })

				// Overwrite the jth key.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]

					// it(`Overwrite ${j}: ${x}`, () => {
					const t = clone(tree)

					await verifyImmutable(tree, async () => {
						await t.set(x, x * 2)
						await verify(t)
					})

					// Check get on all keys.
					for (let k = 0; k <= i; k++) {
						const y = numbers[k]
						if (x === y) assert.equal(await t.get(y), y * 2)
						else assert.equal(await t.get(y), y.toString())
					}
					// })
				}

				// Delete the jth key.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]

					// it(`Delete ${j} : ${x}`, () => {
					const t = clone(tree)
					await verifyImmutable(tree, async () => {
						await t.delete(x)
						await verify(t)
					})

					// Check get on all keys.
					for (let k = 0; k <= i; k++) {
						const y = numbers[k]
						if (x === y) assert.equal(await t.get(y), undefined)
						else assert.equal(await t.get(y), y.toString())
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

function assertNoLocks(tree: AsyncBinaryPlusTree) {
	assert.equal(tree.kv.locks["_locks"].size, 0)
}

async function test(tree: AsyncBinaryPlusTree, str: string) {
	for (const test of parseTests(str)) {
		let label = `${test.op} ${test.n}`
		if (test.comment) label += " // " + test.comment
		it(label, async () => {
			assertNoLocks(tree)
			if (test.op === "+") await tree.set(test.n, test.n.toString())
			if (test.op === "-") await tree.delete(test.n)
			assertNoLocks(tree)

			// TODO: delete is hung on a lock!

			assert.equal(await inspect(tree), test.tree, test.comment)

			const value = test.op === "+" ? test.n.toString() : undefined
			assert.equal(await tree.get(test.n), value, test.comment)

			assert.equal(
				await tree.depth(),
				test.tree.split("\n").length,
				test.comment
			)
		})
	}
}

type Key = string | number
type KeyTree =
	| { keys: Key[]; children?: undefined }
	| { keys: Key[]; children: KeyTree[] }

async function toKeyTree(
	tree: AsyncBinaryPlusTree,
	id = "root"
): Promise<KeyTree> {
	const node = await tree.kv.get(id)
	if (!node) {
		console.warn("Missing node!")
		// throw new Error("Missing node!")
		return { keys: [] }
	}

	const keys = node.values.map((v) => v.key)
	if (node.leaf) return { keys: keys }

	const subtrees = await Promise.all(
		node.values.map((v) => toKeyTree(tree, v.value))
	)
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

async function inspect(tree: AsyncBinaryPlusTree) {
	const keyTree = await toKeyTree(tree)
	const layers = toTreeLayers(keyTree)
	const str = layers
		.map((layer) =>
			layer.length === 1 ? print(layer[0]) : layer.map(print).join(" ")
		)
		.join("\n")
	return str
}

function clone(tree: AsyncBinaryPlusTree) {
	const kv = new TestAsyncKeyValueDatabase() as AsyncKeyValueDatabase
	kv.map = cloneDeep(tree.kv.map)
	const cloned = new AsyncBinaryPlusTree(kv, tree.minSize, tree.maxSize)
	return cloned
}

function shallowClone(tree: AsyncBinaryPlusTree) {
	const kv = new TestAsyncKeyValueDatabase() as AsyncKeyValueDatabase
	kv.map = new Map(tree.kv.map)
	const cloned = new AsyncBinaryPlusTree(kv, tree.minSize, tree.maxSize)
	return cloned
}

/** Check for node sizes. */
async function verify(tree: AsyncBinaryPlusTree, id = "root") {
	const node = await tree.kv.get(id)
	if (id === "root") {
		if (!node) return
		if (node.leaf) return
		for (const { value } of node.values) await verify(tree, value)
		return
	}

	assert.ok(node)
	assert.ok(node.values.length >= tree.minSize)
	assert.ok(node.values.length <= tree.maxSize, await inspect(tree))

	if (node.leaf) return
	for (const { value } of node.values) verify(tree, value)
}

async function verifyImmutable(
	tree: AsyncBinaryPlusTree,
	fn: () => Promise<void>
) {
	const shallow = shallowClone(tree)
	const deep = clone(tree)

	await fn()

	const keys = uniq([...Object.keys(tree.kv.map), ...Object.keys(shallow)])
	for (const key of keys) {
		const newNode = await tree.kv.get(key)
		const originalValue = await deep.kv.get(key)
		const originalRef = await shallow.kv.get(key)

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
