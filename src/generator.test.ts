/*

Generators are kind of confusing, so here's place to start.

*/

import { strict as assert } from "assert"
import { describe, it } from "mocha"

function run<T>(
	fn: () => Generator<number, { sum: number }, { double: number }>
): string {
	const generator = fn()
	let nextValue = generator.next()
	while (!nextValue.done) {
		const double = nextValue.value * 2
		nextValue = generator.next({ double })
	}

	const { sum } = nextValue.value
	return sum.toString()
}

function* f() {
	const { double: a } = yield 1
	assert.equal(a, 2)

	const { double: b } = yield 13
	assert.equal(b, 26)

	return { sum: a + b }
}

async function runAsync(
	generatorFunction: () => AsyncGenerator<
		number,
		{ sum: number },
		{ double: number }
	>
): Promise<string> {
	const generator = generatorFunction()
	let nextValue = await generator.next()

	while (!nextValue.done) {
		const double = nextValue.value * 2
		nextValue = await generator.next({ double })
	}

	const { sum } = nextValue.value
	return sum.toString()
}

async function* g() {
	const { double: a } = yield 1
	assert.equal(a, 2)

	await Promise.resolve()

	const { double: b } = yield 13
	assert.equal(b, 26)

	return { sum: a + b }
}

describe("Generator Demo", () => {
	it("sync", () => {
		const result = run(f)
		assert.equal(result, "28")
	})

	it("async", async () => {
		const result = await runAsync(g)
		assert.equal(result, "28")
	})
})
