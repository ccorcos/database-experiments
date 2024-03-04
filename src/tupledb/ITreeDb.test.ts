import { strict as assert } from "assert"
import { describe, it } from "mocha"
import { ITreeDb } from "./ITreeDb"

describe("ITreeDb", () => {
	it("works", () => {
		const db = new ITreeDb()

		const now = Date.now()
		const hourMs = 1000 * 60 * 60
		const dayMs = hourMs * 24

		const event1 = {
			id: randomId(),
			start: new Date(now).toISOString(),
			end: new Date(now + hourMs).toISOString(),
			name: "Meeting",
		}
		const event2 = {
			id: randomId(),
			start: new Date(now + 2 * dayMs).toISOString(),
			end: new Date(now + 3 * dayMs).toISOString(),
			name: "Party",
		}

		{
			const tx = db.transact()
			tx.set([event1.start, event1.end, event1.id], event1)
			tx.set([event2.start, event2.end, event2.id], event2)
			tx.commit()
		}

		let called = 0
		db.subscribe(
			// Today
			[new Date(now).toISOString(), new Date(now + dayMs).toISOString()],
			() => {
				called += 1
			}
		)

		const event3 = {
			id: randomId(),
			start: new Date(now + 2 * hourMs).toISOString(),
			end: new Date(now + 3 * hourMs).toISOString(),
			name: "Zoom call",
		}
		const event4 = {
			id: randomId(),
			start: new Date(now + 12 * hourMs).toISOString(),
			end: new Date(now + 12.5 * hourMs).toISOString(),
			name: "Dinner",
		}

		{
			const tx = db.transact()
			tx.set([event3.start, event3.end, event3.id], event3)
			tx.set([event4.start, event4.end, event4.id], event4)
			tx.commit()
		}

		assert.equal(called, 1)

		const event5 = {
			id: randomId(),
			start: new Date(now + 36 * hourMs).toISOString(),
			end: new Date(now + 39 * hourMs).toISOString(),
			name: "Later",
		}

		{
			const tx = db.transact()
			tx.set([event5.start, event5.end, event5.id], event5)
			tx.commit()
		}

		assert.equal(called, 1)
	})
})

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}
