import { strict as assert } from "assert"
import { jsonCodec } from "lexicodec"
import { sample, shuffle, uniq } from "lodash"
import { describe, it } from "mocha"
import { InMemoryIntervalTree } from "./InMemoryIntervalTree"

describe("InMemoryIntervalTree", () => {
	it("works", () => {
		type K = [number, number, string]

		const tree = new InMemoryIntervalTree<K, number>(
			3,
			9,
			jsonCodec.compare,
			jsonCodec.compare
		)

		function* makeTuples(min: number, max: number) {
			for (let start = min; start < max; start++) {
				for (let end = start; end < max; end++) {
					const sum = start + end
					const id = sum % 2 === 0 ? "even" : "odd"
					yield { key: [start, end, id] as K, value: sum }
					// Some ranges will have duplicate entries.
					if (sum % 3 === 0) {
						yield { key: [start, end, "third"] as K, value: sum }
					}
				}
			}
		}

		const tuples = Array.from(makeTuples(0, 100))
		for (const { key, value } of shuffle(tuples)) {
			tree.set(key, value)
		}

		for (let start = -19; start < 121; start += 10) {
			for (let end = start; end < 121; end += 10) {
				const result = tree.overlaps({ gte: start, lte: end })
				assert.deepEqual(
					result,
					tuples.filter(({ key: [min, max] }) => start <= max && end >= min)
				)
			}
		}

		// Decimal overlaps.
		for (let start = -1.2; start < 105; start += 10) {
			for (let end = start; end < start + 5; end += 0.4) {
				const result = tree.overlaps({ gte: start, lte: end })
				assert.deepEqual(
					result,
					tuples.filter(({ key: [min, max] }) => start <= max && end >= min)
				)
			}
		}
	})

	it("property test", function () {
		this.timeout(10_000)
		type Key = [[string, string], [string, string], string]

		const size = 300
		const tree = new InMemoryIntervalTree<Key, number>(
			3,
			9,
			jsonCodec.compare,
			jsonCodec.compare
		)

		const tuples: { key: Key; value: number }[] = []

		// Completely random non-numerical bounds with tuple bounds.
		let i = 0
		while (i < size) {
			const [min, max] = [randomId(), randomId()].sort()
			tuples.push({
				key: [["user", min], ["user", max], randomId()],
				value: i++,
			})

			// Same bound
			if (i % 7 === 0) {
				tuples.push({
					key: [["user", min], ["user", max], randomId()],
					value: i++,
				})
			}

			// Half same bound
			if (i % 11 === 0) {
				const [min2, max2] = [min, randomId()].sort()
				tuples.push({
					key: [["user", min2], ["user", max2], randomId()],
					value: i++,
				})
			}
		}

		for (let i = 0; i < tuples.length; i++) {
			const { key, value } = tuples[i]
			tree.set(key, value)

			const answer = (
				args: { gt?: Key[0]; gte?: Key[0]; lt?: Key[0]; lte?: Key[0] } = {}
			) => {
				return tree.list().filter(({ key: [start, end] }) => {
					if (args.gt !== undefined) {
						if (jsonCodec.compare(end, args.gt) <= 0) return false
					} else if (args.gte !== undefined) {
						if (jsonCodec.compare(end, args.gte) < 0) return false
					}
					if (args.lt !== undefined) {
						if (jsonCodec.compare(start, args.lt) >= 0) return false
					} else if (args.lte !== undefined) {
						if (jsonCodec.compare(start, args.lte) > 0) return false
					}
					return true
				})
			}

			const testOverlaps = (
				args: { gt?: Key[0]; gte?: Key[0]; lt?: Key[0]; lte?: Key[0] } = {}
			) => assert.deepEqual(tree.overlaps(args), answer(args))

			const ranges = (n: number) => {
				// Beyond the left and right bounds.
				const left = String.fromCharCode("0".charCodeAt(0) - 1)
				const right = String.fromCharCode("z".charCodeAt(0) + 1)

				// Sample bounds from the existing dataset.
				let bounds: string[] = []
				for (const { key } of tree.list()) {
					const [a, b] = key
					bounds.push(a[1], b[1])
				}
				// Half the elements are existing boundaries, half are new random values.
				bounds = shuffle(uniq(bounds)).slice(0, Math.round(n / 2))
				while (bounds.length < n) bounds.push(randomId())

				const ranges: [Key[0], Key[0]][] = []
				ranges.push([
					["user", left],
					["user", sample(bounds)!],
				])
				ranges.push([
					["user", left],
					["user", randomId()],
				])
				ranges.push([
					["user", sample(bounds)!],
					["user", right],
				])
				ranges.push([
					["user", randomId()],
					["user", right],
				])

				for (let i = 0; i < n - 4; i++) {
					const [min, max] = [sample(bounds)!, sample(bounds)!].sort()
					ranges.push([
						["user", min],
						["user", max],
					])
				}
				return ranges
			}

			testOverlaps()
			for (const [min, max] of ranges(size / 20)) {
				testOverlaps({ gt: min })
				testOverlaps({ gte: min })
				testOverlaps({ lt: min })
				testOverlaps({ lte: min })
				testOverlaps({ gt: max })
				testOverlaps({ gte: max })
				testOverlaps({ lt: max })
				testOverlaps({ lte: max })
				testOverlaps({ gt: min, lt: max })
				testOverlaps({ gt: min, lte: max })
				testOverlaps({ gte: min, lt: max })
				testOverlaps({ gte: min, lte: max })
			}
		}
	})
})

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}
