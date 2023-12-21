import { TestClock } from "@ccorcos/test-clock"
import { strict as assert } from "assert"
import { describe, it } from "mocha"
import { AsyncKeyValueDatabase } from "./kv-lock"

describe("AsyncKeyValueDatabase", () => {
	it("get", async () => {
		const kv = new AsyncKeyValueDatabase<number>()

		let result = await kv.get("a")
		assert.deepEqual(result, undefined)

		await kv.set("a", 1)
		result = await kv.get("a")
		assert.deepEqual(result, 1)
	})

	it("tx run", async () => {
		const kv = new AsyncKeyValueDatabase<number>()

		const { sleep, run } = new TestClock()

		const p1 = kv.tx(async function* () {
			yield { a: "r" }
			await sleep(10)
			return await kv.get("a")
		})

		const p2 = kv.tx(async function* () {
			await sleep(2)
			yield { a: "r" }
			return await kv.get("a")
		})

		const p3 = kv.tx(async function* () {
			await sleep(1)
			yield { a: "rw" }
			await sleep(10)
			await kv.set("a", 1)
		})

		const p4 = kv.tx(async function* () {
			await sleep(3)
			yield { a: "rw" }
			await sleep(10)
			await kv.set("a", 2)
			return true
		})

		const p5 = kv.tx(async function* () {
			await sleep(4)
			yield { a: "r" }
			return await kv.get("a")
		})

		await run()
		const [r1, r2, r3, r4, r5] = await Promise.all([p1, p2, p3, p4, p5])

		assert.deepEqual([r1, r2, r3, r4, r5], [undefined, 1, undefined, true, 2])
	})
})
