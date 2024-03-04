import { strict as assert } from "assert"
import { jsonCodec } from "lexicodec"
import { describe, it } from "mocha"
import { BTreeDb } from "./BTreeDb"

describe("BTreeDb", () => {
	it("works", () => {
		const db = new BTreeDb()

		const user1 = { id: randomId(), name: "Chet" }
		const user2 = { id: randomId(), name: "Simon" }

		{
			const tx = db.transact()
			tx.set(["user", user1.id], user1)
			tx.set(["user", user2.id], user2)
			tx.commit()
		}

		let called = 0
		db.subscribe(
			[
				["user", jsonCodec.MIN],
				["user", jsonCodec.MAX],
			],
			() => {
				called += 1
			}
		)

		const user3 = { id: randomId(), name: "Rob" }
		const user4 = { id: randomId(), name: "Tanishq" }

		{
			const tx = db.transact()
			tx.set(["user", user3.id], user3)
			tx.set(["user", user4.id], user4)
			tx.commit()
		}

		assert.equal(called, 1)
	})
})

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}
