import * as t from "data-type-ts"

const Todo = t.object({
	id: t.string,
	text: t.string,
	createdAt: t.string,
	completed: t.boolean,
})

function query<A extends any[], R>(
	fn: (tx: any, ...args: A) => Generator<any, R>
) {
	return fn
}

const createTodo = query(function* (tx) {
	return
})
