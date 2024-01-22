import { strict as assert } from "assert"
import { jsonCodec } from "lexicodec"
import { shuffle } from "lodash"
import { describe, it } from "mocha"
import { BinaryPlusIntervalTree } from "./itree"

describe("BinaryPlusIntervalTree", () => {
	it("works", () => {
		type K = [number, number, string]

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

		const reduceInterval = (a: [number, number], b: [number, number]) => {
			return [Math.min(a[0], b[0]), Math.max(a[1], b[1])] as [number, number]
		}

		const tree = new BinaryPlusIntervalTree(
			3,
			9,
			reduceInterval,
			jsonCodec.compare,
			jsonCodec.compare
		)

		for (const { key, value } of shuffle(tuples)) {
			tree.set(key, value)
		}

		for (let start = -19; start < 121; start += 10) {
			for (let end = start; end < 121; end += 10) {
				const result = tree.overlaps([start, end])
				assert.deepEqual(
					result,
					tuples.filter(({ key: [min, max] }) => start <= max && end >= min)
				)
			}
		}

		// Decimal overlaps.
		for (let start = -1.2; start < 105; start += 10) {
			for (let end = start; end < start + 5; end += 0.4) {
				const result = tree.overlaps([start, end])
				assert.deepEqual(
					result,
					tuples.filter(({ key: [min, max] }) => start <= max && end >= min)
				)
			}
		}
	})
})
