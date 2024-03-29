import { search } from "@ccorcos/ordered-array"
import { strict as assert } from "assert"
import { jsonCodec } from "lexicodec"
import { cloneDeep, max, min, sample, sum, uniq, uniqWith } from "lodash"
import { describe, it } from "mocha"
import {
	InMemoryReducerTree,
	TreeReducer,
	combineTreeReducers,
} from "./InMemoryReducerTree"

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

const combined = combineTreeReducers({
	count,
	min: minValue,
	max: maxValue,
})

describe("InMemoryReducerTree", () => {
	describe("structural tests 2-4", () => {
		const tree = new InMemoryReducerTree(2, 4, count)
		test(tree, structuralTests24)
	})

	describe("property test 2-4 * 100", () => {
		propertyTest({ minSize: 2, maxSize: 4, testSize: 100 })
	})

	describe("property test 3-6 * 100", () => {
		propertyTest({ minSize: 3, maxSize: 6, testSize: 100 })
	})

	function propertyTest(args: {
		minSize: number
		maxSize: number
		testSize: number
	}) {
		const size = args.testSize
		const numbers = randomInts(size)

		const tree = new InMemoryReducerTree<string | number, number, number>(
			args.minSize,
			args.maxSize,
			sumReducer
		)
		for (let i = 0; i < size; i++) {
			const n = numbers[i]
			it(`Set ${i} : ${n}`, () => {
				tree.set(n, n)
				verify(tree)
				verifySum(tree)

				// Get works on every key so far.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]
					assert.equal(tree.get(x), x)
				}

				// Overwrite the jth key.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]
					const t = cloneTree(tree)
					t.set(x, x * 2)
					verify(t)
					verifySum(t)

					// Check get on all keys.
					for (let k = 0; k <= i; k++) {
						const y = numbers[k]
						if (x === y) assert.equal(t.get(y), y * 2)
						else assert.equal(t.get(y), y)
					}
				}

				// Delete the jth key.
				for (let j = 0; j <= i; j++) {
					const x = numbers[j]

					const t = cloneTree(tree)
					t.delete(x)
					verify(t)
					verifySum(t)

					// Check get on all keys.
					for (let k = 0; k <= i; k++) {
						const y = numbers[k]
						if (x === y) assert.equal(t.get(y), undefined)
						else assert.equal(t.get(y), y)
					}
				}
			})
		}
	}

	it("big tree", () => {
		const numbers = randomInts(20_000)
		const tree = new InMemoryReducerTree(3, 9, count)
		for (const number of numbers) {
			tree.set(number, number * 2)
			assert.equal(tree.get(number), number * 2)
		}
		for (const number of numbers) {
			tree.delete(number)
			assert.equal(tree.get(number), undefined)
		}
		assert.equal(treeDepth(tree), 1)
	})

	it("tuple keys", () => {
		const tree = new InMemoryReducerTree<any[], any>(
			3,
			9,
			count,
			jsonCodec.compare
		)

		const numbers = randomInts(2000)
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

	describe("list", () => {
		const listTest =
			(tree, min, max) =>
			(
				args: {
					gt?: number
					gte?: number
					lt?: number
					lte?: number
					limit?: number
					reverse?: boolean
				} = {}
			) => {
				assert.deepEqual(
					tree.list(args),
					listEvens(min, max)(args),
					JSON.stringify(args)
				)
			}

		it("manual tests", () => {
			// All even numbers from 0 to 1998

			// Test a few different tree sizes.
			for (const [minSize, maxSize] of [
				[3, 9],
				[8, 21],
				[50, 100],
			]) {
				const tree = new InMemoryReducerTree(minSize, maxSize, count)
				for (const { key, value } of listEvens(0, 1998)()) tree.set(key, value)

				const testList = listTest(tree, 0, 1998)

				// Entire thing
				testList()
				testList({ limit: 2 })
				testList({ limit: 40 })
				testList({ reverse: true })
				testList({ reverse: true, limit: 40 })
				testList({ reverse: true, limit: 4 })

				// Less than odd.
				testList({ lt: 9 })
				testList({ lt: 9, limit: 2 })
				testList({ lt: 9, reverse: true })
				testList({ lt: 9, reverse: true, limit: 2 })
				testList({ lt: 199 })
				testList({ lt: 199, limit: 40 })
				testList({ lt: 199, reverse: true })
				testList({ lt: 199, reverse: true, limit: 40 })

				// Less than open bound.
				testList({ lt: 10 })
				testList({ lt: 10, limit: 2 })
				testList({ lt: 10, reverse: true })
				testList({ lt: 10, reverse: true, limit: 2 })
				testList({ lt: 200 })
				testList({ lt: 200, limit: 40 })
				testList({ lt: 200, reverse: true })
				testList({ lt: 200, reverse: true, limit: 40 })

				// Less than odd closed bound.
				testList({ lte: 9 })
				testList({ lte: 9, limit: 2 })
				testList({ lte: 9, reverse: true })
				testList({ lte: 9, reverse: true, limit: 2 })
				testList({ lte: 199 })
				testList({ lte: 199, limit: 40 })
				testList({ lte: 199, reverse: true })
				testList({ lte: 199, reverse: true, limit: 40 })

				// Less than closed bound.
				testList({ lte: 10 })
				testList({ lte: 10, limit: 2 })
				testList({ lte: 10, reverse: true })
				testList({ lte: 10, reverse: true, limit: 2 })
				testList({ lte: 200 })
				testList({ lte: 200, limit: 40 })
				testList({ lte: 200, reverse: true })
				testList({ lte: 200, reverse: true, limit: 40 })

				// Less than left bound.
				testList({ lt: -1 })
				testList({ lt: -1, limit: 2 })
				testList({ lt: -1, reverse: true })
				testList({ lt: -1, reverse: true, limit: 2 })
				testList({ lte: -1 })
				testList({ lte: -1, limit: 2 })
				testList({ lte: -1, reverse: true })
				testList({ lte: -1, reverse: true, limit: 2 })

				// Less than right bound
				testList({ lt: 5000 })
				testList({ lt: 5000, limit: 2 })
				testList({ lt: 5000, reverse: true })
				testList({ lt: 5000, reverse: true, limit: 2 })
				testList({ lte: 5000 })
				testList({ lte: 5000, limit: 2 })
				testList({ lte: 5000, reverse: true })
				testList({ lte: 5000, reverse: true, limit: 2 })

				// Greater than odd.
				testList({ gt: 1989 })
				testList({ gt: 1989, limit: 2 })
				testList({ gt: 1989, reverse: true })
				testList({ gt: 1989, reverse: true, limit: 2 })
				testList({ gt: 1781 })
				testList({ gt: 1781, limit: 40 })
				testList({ gt: 1781, reverse: true })
				testList({ gt: 1781, reverse: true, limit: 40 })

				// Greater than open bound.
				testList({ gt: 1988 })
				testList({ gt: 1988, limit: 2 })
				testList({ gt: 1988, reverse: true })
				testList({ gt: 1988, reverse: true, limit: 2 })
				testList({ gt: 1780 })
				testList({ gt: 1780, limit: 40 })
				testList({ gt: 1780, reverse: true })
				testList({ gt: 1780, reverse: true, limit: 40 })

				// Greater than odd closed bound
				testList({ gte: 1989 })
				testList({ gte: 1989, limit: 2 })
				testList({ gte: 1989, reverse: true })
				testList({ gte: 1989, reverse: true, limit: 2 })
				testList({ gte: 1781 })
				testList({ gte: 1781, limit: 40 })
				testList({ gte: 1781, reverse: true })
				testList({ gte: 1781, reverse: true, limit: 40 })

				// Greater than closed bound.
				testList({ gte: 1988 })
				testList({ gte: 1988, limit: 2 })
				testList({ gte: 1988, reverse: true })
				testList({ gte: 1988, reverse: true, limit: 2 })
				testList({ gte: 1780 })
				testList({ gte: 1780, limit: 40 })
				testList({ gte: 1780, reverse: true })
				testList({ gte: 1780, reverse: true, limit: 40 })

				// Greater than left bound.
				testList({ gt: -1 })
				testList({ gt: -1, limit: 2 })
				testList({ gt: -1, reverse: true })
				testList({ gt: -1, reverse: true, limit: 2 })
				testList({ gte: -1 })
				testList({ gte: -1, limit: 2 })
				testList({ gte: -1, reverse: true })
				testList({ gte: -1, reverse: true, limit: 2 })

				// Greater than right bound
				testList({ gt: 5000 })
				testList({ gt: 5000, limit: 2 })
				testList({ gt: 5000, reverse: true })
				testList({ gt: 5000, reverse: true, limit: 2 })
				testList({ gte: 5000 })
				testList({ gte: 5000, limit: 2 })
				testList({ gte: 5000, reverse: true })
				testList({ gte: 5000, reverse: true, limit: 2 })

				// Within a branch

				let sameLeaf = (args: { reverse?: boolean; limit?: number } = {}) => {
					testList({ gt: 2, lt: 8, ...args })
					testList({ gte: 2, lt: 8, ...args })
					testList({ gt: 2, lte: 8, ...args })
					testList({ gte: 2, lte: 8, ...args })

					testList({ gt: 204, lt: 208, ...args })
					testList({ gte: 204, lt: 208, ...args })
					testList({ gt: 204, lte: 208, ...args })
					testList({ gte: 204, lte: 208, ...args })

					testList({ gt: 206, lt: 210, ...args })
					testList({ gte: 206, lt: 210, ...args })
					testList({ gt: 206, lte: 210, ...args })
					testList({ gte: 206, lte: 210, ...args })

					testList({ gt: 210, lt: 214, ...args })
					testList({ gte: 210, lt: 214, ...args })
					testList({ gt: 210, lte: 214, ...args })
					testList({ gte: 210, lte: 214, ...args })
				}
				sameLeaf()
				sameLeaf({ limit: 2 })
				sameLeaf({ reverse: true })
				sameLeaf({ reverse: true, limit: 2 })

				let differentLeaves = (
					args: { reverse?: boolean; limit?: number } = {}
				) => {
					testList({ gt: 200, lt: 800, ...args })
					testList({ gte: 200, lt: 800, ...args })
					testList({ gt: 200, lte: 800, ...args })
					testList({ gte: 200, lte: 800, ...args })

					testList({ gt: 204, lt: 808, ...args })
					testList({ gte: 204, lt: 808, ...args })
					testList({ gt: 204, lte: 808, ...args })
					testList({ gte: 204, lte: 808, ...args })
				}
				differentLeaves()
				differentLeaves({ limit: 20 })
				differentLeaves({ reverse: true })
				differentLeaves({ reverse: true, limit: 20 })

				let bounds = (args: { reverse?: boolean; limit?: number } = {}) => {
					testList({ gt: -100, lt: 100, ...args })
					testList({ gte: -100, lt: 100, ...args })
					testList({ gt: -100, lte: 100, ...args })
					testList({ gte: -100, lte: 100, ...args })

					testList({ gt: 1900, lt: 2100, ...args })
					testList({ gte: 1900, lt: 2100, ...args })
					testList({ gt: 1900, lte: 2100, ...args })
					testList({ gte: 1900, lte: 2100, ...args })

					testList({ gt: -100, lt: 2100, ...args })
					testList({ gte: -100, lt: 2100, ...args })
					testList({ gt: -100, lte: 2100, ...args })
					testList({ gte: -100, lte: 2100, ...args })
				}
				bounds()
				bounds({ limit: 20 })
				bounds({ reverse: true })
				bounds({ reverse: true, limit: 20 })

				// Random challenges from property test.
				testList({ gt: -91, lt: 0 })
				testList({ gt: 2, lt: 3 })
			}
		})

		it("property tests", () => {
			const tree = new InMemoryReducerTree(3, 9, count)

			const min = 0
			const max = 400
			const delta = 20

			for (const { key, value } of listEvens(min, max)()) tree.set(key, value)

			const testList = listTest(tree, min, max)

			for (let start = -min - delta; start < max + delta; start += 3) {
				for (let end = start + 1; end < max + delta; end += 5) {
					testList({ gt: start, lt: end })
					testList({ gt: start, lt: end, limit: 2 })
					testList({ gt: start, lt: end, limit: 40 })
					testList({ gt: start, lt: end, reverse: true })
					testList({ gt: start, lt: end, reverse: true, limit: 2 })
					testList({ gt: start, lt: end, reverse: true, limit: 40 })
					testList({ gte: start, lt: end })
					testList({ gte: start, lt: end, limit: 2 })
					testList({ gte: start, lt: end, limit: 40 })
					testList({ gte: start, lt: end, reverse: true })
					testList({ gte: start, lt: end, reverse: true, limit: 2 })
					testList({ gte: start, lt: end, reverse: true, limit: 40 })
					testList({ gt: start, lte: end })
					testList({ gt: start, lte: end, limit: 2 })
					testList({ gt: start, lte: end, limit: 40 })
					testList({ gt: start, lte: end, reverse: true })
					testList({ gt: start, lte: end, reverse: true, limit: 2 })
					testList({ gt: start, lte: end, reverse: true, limit: 40 })
					testList({ gte: start, lte: end })
					testList({ gte: start, lte: end, limit: 2 })
					testList({ gte: start, lte: end, limit: 40 })
					testList({ gte: start, lte: end, reverse: true })
					testList({ gte: start, lte: end, reverse: true, limit: 2 })
					testList({ gte: start, lte: end, reverse: true, limit: 40 })
				}
			}
		})

		it("smaller property tests", () => {
			const tree = new InMemoryReducerTree(3, 9, count)

			const min = 0
			const max = 100
			const delta = 10

			for (const { key, value } of listEvens(min, max)()) tree.set(key, value)

			const testList = listTest(tree, min, max)

			for (let start = -min - delta; start < max + delta; start += 1) {
				for (let end = start; end < max + delta; end += 1) {
					if (start !== end) {
						testList({ gt: start, lt: end })
						testList({ gt: start, lt: end, limit: 1 })
						testList({ gt: start, lt: end, limit: 2 })
						testList({ gt: start, lt: end, limit: 40 })
						testList({ gt: start, lt: end, reverse: true })
						testList({ gt: start, lt: end, reverse: true, limit: 1 })
						testList({ gt: start, lt: end, reverse: true, limit: 2 })
						testList({ gt: start, lt: end, reverse: true, limit: 40 })
						testList({ gte: start, lt: end })
						testList({ gte: start, lt: end, limit: 1 })
						testList({ gte: start, lt: end, limit: 2 })
						testList({ gte: start, lt: end, limit: 40 })
						testList({ gte: start, lt: end, reverse: true })
						testList({ gte: start, lt: end, reverse: true, limit: 1 })
						testList({ gte: start, lt: end, reverse: true, limit: 2 })
						testList({ gte: start, lt: end, reverse: true, limit: 40 })
						testList({ gt: start, lte: end })
						testList({ gt: start, lte: end, limit: 2 })
						testList({ gt: start, lte: end, limit: 40 })
						testList({ gt: start, lte: end, reverse: true })
						testList({ gt: start, lte: end, reverse: true, limit: 1 })
						testList({ gt: start, lte: end, reverse: true, limit: 2 })
						testList({ gt: start, lte: end, reverse: true, limit: 40 })
					}
					testList({ gte: start, lte: end })
					testList({ gte: start, lte: end, limit: 1 })
					testList({ gte: start, lte: end, limit: 2 })
					testList({ gte: start, lte: end, limit: 40 })
					testList({ gte: start, lte: end, reverse: true })
					testList({ gte: start, lte: end, reverse: true, limit: 1 })
					testList({ gte: start, lte: end, reverse: true, limit: 2 })
					testList({ gte: start, lte: end, reverse: true, limit: 40 })
				}
			}
		})
	})

	describe("reduce", function () {
		this.timeout(20_000)

		const countTest =
			(tree: InMemoryReducerTree, min: number, max: number) =>
			(
				args: {
					gt?: number
					gte?: number
					lt?: number
					lte?: number
				} = {}
			) => {
				assert.deepEqual(
					tree.reduce(args),
					listEvens(min, max)(args).length,
					JSON.stringify(args)
				)
			}

		// Similar to list tests.
		it("manual tests", () => {
			// Test a few different tree sizes.
			for (const [minSize, maxSize] of [
				[3, 9],
				[8, 21],
				[50, 100],
			]) {
				const tree = new InMemoryReducerTree(minSize, maxSize, count)
				for (const { key, value } of listEvens(0, 1998)()) tree.set(key, value)

				const testReduce = countTest(tree, 0, 1998)

				// Entire thing
				testReduce()

				// Less than odd.
				testReduce({ lt: 9 })
				testReduce({ lt: 199 })

				// Less than open bound.
				testReduce({ lt: 10 })
				testReduce({ lt: 200 })

				// Less than odd closed bound.
				testReduce({ lte: 9 })
				testReduce({ lte: 199 })

				// Less than closed bound.
				testReduce({ lte: 10 })
				testReduce({ lte: 200 })

				// Less than left bound.
				testReduce({ lt: -1 })
				testReduce({ lte: -1 })

				// Less than right bound
				testReduce({ lt: 5000 })
				testReduce({ lte: 5000 })

				// Greater than odd.
				testReduce({ gt: 1989 })
				testReduce({ gt: 1781 })

				// Greater than open bound.
				testReduce({ gt: 1988 })
				testReduce({ gt: 1780 })

				// Greater than odd closed bound
				testReduce({ gte: 1989 })
				testReduce({ gte: 1781 })

				// Greater than closed bound.
				testReduce({ gte: 1988 })
				testReduce({ gte: 1780 })

				// Greater than left bound.
				testReduce({ gt: -1 })
				testReduce({ gte: -1 })

				// Greater than right bound
				testReduce({ gt: 5000 })
				testReduce({ gte: 5000 })

				// Within the same leaf
				testReduce({ gt: 2, lt: 8 })
				testReduce({ gte: 2, lt: 8 })
				testReduce({ gt: 2, lte: 8 })
				testReduce({ gte: 2, lte: 8 })

				testReduce({ gt: 204, lt: 208 })
				testReduce({ gte: 204, lt: 208 })
				testReduce({ gt: 204, lte: 208 })
				testReduce({ gte: 204, lte: 208 })

				testReduce({ gt: 206, lt: 210 })
				testReduce({ gte: 206, lt: 210 })
				testReduce({ gt: 206, lte: 210 })
				testReduce({ gte: 206, lte: 210 })

				testReduce({ gt: 210, lt: 214 })
				testReduce({ gte: 210, lt: 214 })
				testReduce({ gt: 210, lte: 214 })
				testReduce({ gte: 210, lte: 214 })

				// Within different leaves
				testReduce({ gt: 200, lt: 800 })
				testReduce({ gte: 200, lt: 800 })
				testReduce({ gt: 200, lte: 800 })
				testReduce({ gte: 200, lte: 800 })

				testReduce({ gt: 204, lt: 808 })
				testReduce({ gte: 204, lt: 808 })
				testReduce({ gt: 204, lte: 808 })
				testReduce({ gte: 204, lte: 808 })

				// Bounds conditions
				testReduce({ gt: -100, lt: 100 })
				testReduce({ gte: -100, lt: 100 })
				testReduce({ gt: -100, lte: 100 })
				testReduce({ gte: -100, lte: 100 })

				testReduce({ gt: 1900, lt: 2100 })
				testReduce({ gte: 1900, lt: 2100 })
				testReduce({ gt: 1900, lte: 2100 })
				testReduce({ gte: 1900, lte: 2100 })

				testReduce({ gt: -100, lt: 2100 })
				testReduce({ gte: -100, lt: 2100 })
				testReduce({ gt: -100, lte: 2100 })
				testReduce({ gte: -100, lte: 2100 })

				// Random challenges from property test.
				testReduce({ gt: -91, lt: 0 })
				testReduce({ gt: 2, lt: 3 })
			}
		})

		it("property tests", () => {
			const tree = new InMemoryReducerTree(3, 9, count)

			const min = 0
			const max = 400
			const delta = 20

			for (const { key, value } of listEvens(min, max)()) tree.set(key, value)

			const testReduce = countTest(tree, min, max)

			for (let start = -min - delta; start < max + delta; start += 3) {
				for (let end = start + 1; end < max + delta; end += 5) {
					testReduce({ gt: start, lt: end })
					testReduce({ gte: start, lt: end })
					testReduce({ gt: start, lte: end })
					testReduce({ gte: start, lte: end })
				}
			}
		})

		it("smaller property tests", () => {
			const tree = new InMemoryReducerTree(3, 9, count)

			const min = 0
			const max = 100
			const delta = 10

			for (const { key, value } of listEvens(min, max)()) tree.set(key, value)

			const testReduce = countTest(tree, min, max)

			for (let start = -min - delta; start < max + delta; start += 1) {
				for (let end = start; end < max + delta; end += 1) {
					if (start !== end) {
						testReduce({ gt: start, lt: end })
						testReduce({ gte: start, lt: end })
						testReduce({ gt: start, lte: end })
					}
					testReduce({ gte: start, lte: end })
				}
			}
		})

		it("combined reducer tuple property test", () => {
			const randomIntTuple = (range: [number, number]) => {
				const size = Math.ceil(randomNumber([1, 6]))
				return randomInts(size, range)
			}
			const randomDecimalTuple = (range: [number, number]) => {
				const size = Math.floor(randomNumber([1, 6]))
				return randomNumbers(size, range)
			}

			const bound = [-10, 10] as [number, number]
			const doubleBound = [-20, 20] as [number, number]

			// Make 2000 random integer tuples of varying length.
			let tuples = Array(2000)
				.fill(0)
				.map(() => randomIntTuple(bound))

			tuples = uniqWith(tuples, (a, b) => jsonCodec.compare(a, b) === 0)

			const tree = new InMemoryReducerTree(3, 9, combined, jsonCodec.compare)

			// Value is the sum of the tuple components.
			for (const tuple of tuples) tree.set(tuple, sum(tuple))

			// Construct 2000 random ranges.
			let ranges = Array(2000)
				.fill(0)
				.map(
					() =>
						[
							randomDecimalTuple(doubleBound),
							randomDecimalTuple(doubleBound),
						] as [number[], number[]]
				)

			// Create ranges samples from the original tuple.a
			for (let i = 0; i < 2000; i++)
				ranges.push([sample(tuples), sample(tuples)] as [number[], number[]])

			// Sort the ranges properly
			ranges = ranges.map((range) => {
				range.sort(jsonCodec.compare)
				return range
			})

			// Ignore ranges where start and end are the same.
			ranges = ranges.filter(([a, b]) => jsonCodec.compare(a, b) !== 0)

			tuples.sort(jsonCodec.compare)

			const answer = (
				args: {
					gt?: number[]
					gte?: number[]
					lt?: number[]
					lte?: number[]
				} = {}
			) => {
				let startIndex = 0
				if (args.gt !== undefined) {
					const result = search(
						tuples,
						args.gt,
						(key) => key,
						jsonCodec.compare
					)
					if (result.found !== undefined) {
						startIndex = result.found + 1
					} else {
						startIndex = result.closest
					}
				}
				if (args.gte !== undefined) {
					const result = search(
						tuples,
						args.gte,
						(key) => key,
						jsonCodec.compare
					)
					if (result.found !== undefined) {
						startIndex = result.found
					} else {
						startIndex = result.closest
					}
				}

				let endIndex = tuples.length
				if (args.lt !== undefined) {
					const result = search(
						tuples,
						args.lt,
						(key) => key,
						jsonCodec.compare
					)
					if (result.found !== undefined) {
						endIndex = result.found
					} else {
						endIndex = result.closest
					}
				}
				if (args.lte !== undefined) {
					const result = search(
						tuples,
						args.lte,
						(key) => key,
						jsonCodec.compare
					)
					if (result.found !== undefined) {
						endIndex = result.found + 1
					} else {
						endIndex = result.closest
					}
				}

				const results = tuples.slice(startIndex, endIndex)

				const values = results.map((t) => sum(t))
				const data = {
					count: results.length,
					min: min(values),
					max: max(values),
				}

				return data
			}

			const testReduce = (
				args: {
					gt?: number[]
					gte?: number[]
					lt?: number[]
					lte?: number[]
				} = {}
			) => {
				assert.deepEqual(tree.reduce(args), answer(args))
			}

			testReduce()
			for (const [start, end] of ranges) {
				testReduce({ gt: start })
				testReduce({ gte: start })
				testReduce({ lt: start })
				testReduce({ lte: start })
				testReduce({ gt: start, lt: end })
				testReduce({ gt: start, lte: end })
				testReduce({ gte: start, lt: end })
				testReduce({ gte: start, lte: end })
			}
		})
	})
})

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

function cloneTree<K, V, D>(tree: InMemoryReducerTree<K, V, D>) {
	const cloned = new InMemoryReducerTree<K, V, D>(
		tree.minSize,
		tree.maxSize,
		tree.reducer,
		tree.compareKey
	)
	cloned.nodes = cloneDeep(tree.nodes)
	return cloned
}

function treeDepth(tree: InMemoryReducerTree) {
	const root = tree.nodes.get("root")
	if (!root) return 0
	let depth = 1
	let node = root
	while (!node.leaf) {
		depth += 1
		const nextNode = tree.nodes.get(node.children[0].childId)
		if (!nextNode) throw new Error("Broken.")
		node = nextNode
	}
	return depth
}

function test(tree: InMemoryReducerTree, str: string) {
	for (const test of parseTests(structuralTests24)) {
		let label = `${test.op} ${test.n}`
		if (test.comment) label += " // " + test.comment
		it(label, () => {
			if (test.op === "+") tree.set(test.n, test.n.toString())
			if (test.op === "-") tree.delete(test.n)
			assert.equal(inspect(tree), test.tree, test.comment)

			const value = test.op === "+" ? test.n.toString() : undefined
			assert.equal(tree.get(test.n), value, test.comment)

			assert.equal(treeDepth(tree), test.tree.split("\n").length, test.comment)

			verify(tree)
		})
	}
}

type Key = string | number | null
type KeyTree =
	| { keys: Key[]; children?: undefined }
	| { keys: Key[]; children: KeyTree[] }

function toKeyTree(tree: InMemoryReducerTree, id = "root"): KeyTree {
	const node = tree.nodes.get(id)
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

function inspect(tree: InMemoryReducerTree) {
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
function verify(tree: InMemoryReducerTree, id = "root") {
	const node = tree.nodes.get(id)
	if (id === "root") {
		assert.equal(countNodes(tree), tree.nodes.size)
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

function verifySum(
	tree: InMemoryReducerTree<any, number, number>,
	id = "root"
): number {
	const node = tree.nodes.get(id)
	if (!node) return 0

	if (node.leaf) {
		assert.equal(node.data, sum(node.values.map((x) => x.value)))
		return node.data
	}

	let branchCount = 0
	for (const child of node.children) {
		const childCount = verifySum(tree, child.childId)
		assert.equal(
			child.data,
			childCount,
			[
				"child count",
				"minKeys",
				JSON.stringify(node.children.map((child) => child.minKey)),
				"item.data",
				child.data,
				"node count",
				childCount,
				"tree",
				inspect(tree),
			].join("\n")
		)
		branchCount += child.data
	}
	return branchCount
}

function countNodes(tree: InMemoryReducerTree, id = "root") {
	const node = tree.nodes.get(id)
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

function listEvens(min: number, max: number) {
	return (
		args: {
			gt?: number
			gte?: number
			lt?: number
			lte?: number
			limit?: number
			reverse?: boolean
		} = {}
	) => {
		let start: number
		if (args.gt !== undefined && args.gt % 2 === 0) {
			start = args.gt + 2
		} else if (args.gte !== undefined && args.gte % 2 === 0) {
			start = args.gte
		} else if (args.gt !== undefined || args.gte !== undefined) {
			const above = Math.ceil((args.gt || args.gte) as number)
			if (above % 2 === 0) start = above
			else start = above + 1
		} else {
			start = 0
		}

		start = Math.max(start, min)

		let end: number
		if (args.lt !== undefined && args.lt % 2 === 0) {
			end = args.lt - 2
		} else if (args.lte !== undefined && args.lte % 2 === 0) {
			end = args.lte
		} else if (args.lt !== undefined || args.lte !== undefined) {
			const above = Math.floor((args.lt || args.lte) as number)
			if (above % 2 === 0) end = above
			else end = above - 1
		} else {
			end = 1998
		}

		end = Math.min(end, max)

		let count = 0
		const result: { key: number; value: number }[] = []
		if (args.reverse) {
			for (let i = end; i >= start; i -= 2) {
				count += 1
				result.push({ key: i, value: i })
				if (args.limit && count >= args.limit) {
					break
				}
			}
		} else {
			for (let i = start; i <= end; i += 2) {
				count += 1
				result.push({ key: i, value: i })
				if (args.limit && count >= args.limit) {
					break
				}
			}
		}
		return result
	}
}
