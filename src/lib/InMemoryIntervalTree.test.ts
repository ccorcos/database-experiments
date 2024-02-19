import { strict as assert } from "assert"
import { jsonCodec } from "lexicodec"
import { shuffle } from "lodash"
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
})
