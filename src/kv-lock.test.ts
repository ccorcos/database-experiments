import { TestClock } from "@ccorcos/test-clock"
import { strict as assert } from "assert"
import { describe, it } from "mocha"
import { AsyncKeyValueDatabase } from "./kv-lock"

describe("AsyncKeyValueDatabase", () => {
	it("get", async () => {
		const kv = new AsyncKeyValueDatabase<number>()

		let result = await kv.get("a")
		assert.deepEqual(result, undefined)

		await kv.write({ set: [{ key: "a", value: 1 }] })
		result = await kv.get("a")
		assert.deepEqual(result, 1)
	})

	it("concurrency", async () => {
		const kv = new AsyncKeyValueDatabase<number>()

		const { sleep, run } = new TestClock()

		const p1 = (async () => {
			const tx = kv.transact()

			await tx.readLock("a")
			await sleep(10)

			const result = await kv.get("a")
			tx.release()
			return result
		})()

		const p2 = (async () => {
			const tx = kv.transact()

			await sleep(2)
			await tx.readLock("a")

			const result = await kv.get("a")
			tx.release()
			return result
		})()

		const p3 = (async () => {
			const tx = kv.transact()

			await sleep(1)
			await tx.writeLock("a")

			await sleep(10)
			await tx.set("a", 1)
			await tx.commit()
		})()

		const p4 = (async () => {
			const tx = kv.transact()

			await sleep(3)
			await tx.writeLock("a")

			await sleep(10)
			await tx.set("a", 2)
			await tx.commit()
			return true
		})()

		const p5 = (async () => {
			const tx = kv.transact()

			await sleep(4)
			await tx.readLock("a")

			const result = await kv.get("a")
			tx.release()
			return result
		})()

		await run()
		const [r1, r2, r3, r4, r5] = await Promise.all([p1, p2, p3, p4, p5])

		assert.deepEqual([r1, r2, r3, r4, r5], [undefined, 1, undefined, true, 2])
	})
})
