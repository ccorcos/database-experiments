import { TestClock } from "@ccorcos/test-clock"
import { strict as assert } from "assert"
import { describe, it } from "mocha"
import { ConcurrencyLocks } from "./concurrency"

describe("ConcurrencyLocks", () => {
	it("run", async () => {
		const map = {}
		const locks = new ConcurrencyLocks()

		const { sleep, run } = new TestClock()

		const p1 = locks.run(async function* () {
			yield { a: "r" }
			await sleep(10)
			return map["a"]
		})

		const p2 = locks.run(async function* () {
			await sleep(2)
			yield { a: "r" }
			return map["a"]
		})

		const p3 = locks.run(async function* () {
			await sleep(1)
			yield { a: "rw" }
			await sleep(10)
			map["a"] = 1
		})

		const p4 = locks.run(async function* () {
			await sleep(3)
			yield { a: "rw" }
			await sleep(10)
			map["a"] = 2
			return true
		})

		const p5 = locks.run(async function* () {
			await sleep(4)
			yield { a: "r" }
			return map["a"]
		})

		await run()
		const [r1, r2, r3, r4, r5] = await Promise.all([p1, p2, p3, p4, p5])

		assert.deepEqual([r1, r2, r3, r4, r5], [undefined, 1, undefined, true, 2])
	})
})
