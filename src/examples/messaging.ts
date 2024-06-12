import * as t from "./dataTypes"

const schema = {
	user: t.object({
		id: t.uuid,
		username: t.string,
	}),
	thread: t.object({
		id: t.uuid,
		created_at: t.datetime,
		created_by: t.uuid,
		member_ids: t.array(t.uuid),
		subject: t.string,
		deleted: t.optional(t.boolean),
	}),
	message: t.object({
		id: t.uuid,
		thread_id: t.uuid,
		author_id: t.uuid,
		created_at: t.datetime,
		text: t.string,
		deleted: t.optional(t.boolean),
	}),
}

type Schema = { [K in keyof typeof schema]: t.Infer<(typeof schema)[K]> }

type Args = {
	[K in keyof Tx]: { fn: K; args: Parameters<Tx[K]> }
}

type Tx = {
	get<T extends keyof Schema>(table: T, id: string): Schema[T] | undefined
	set<T extends keyof Schema>(table: T, id: string, value: Schema[T]): void
	delete<T extends keyof Schema>(table: T, id: string, value: Schema[T]): void
}

type Argify<T extends { [fn: string]: any }> = {
	[K in keyof T]: { fn: K; args: Parameters<T[K]> }
}[keyof T]

type Genify<T extends { [fn: string]: any }> = {
	[K in keyof T]: (
		...args: Parameters<T[K]>
	) => Generator<Argify<T>, ReturnType<T[K]>, unknown>
}

type Tx2 = Genify<Tx>

function* insert<T extends keyof Schema>(tx: Tx2, table: T, value: Schema[T]) {
	const error = schema[table].validate(value)
	if (error) throw new Error(t.formatError(error))

	const m = yield* tx.get("message", "12")
	// const x = yield db.set([table, value.id], value)
}

function* constant<A>(a: A): Generator<A, A, A> {
	return yield a
}

// function remove(table: keyof Schema, id: string) {
// 	const existing = db.get([table, "byId", id])
// 	if (!existing) return

// 	db.delete([table, "byId", id])

// 	const tableIndexes = indexes[table]
// 	if (tableIndexes) {
// 		for (const [index, columns] of Object.entries(tableIndexes)) {
// 			if (index === "byId") continue
// 			const keys = columns.map((col) => existing[col])
// 			db.delete([table, index, ...keys])
// 		}
// 	}
// }

// async getUserByUsername(username: string) {

// async getRecord<T extends RecordTable>(pointer: RecordPointer<T>) {
// async getRecords(pointers: RecordPointer[]): Promise<RecordMap> {
// async getPassword(userId: string) {
// async searchUsers(query: string) {
// async getThreadIds(userId: string, limit: number): Promise<string[]> {
// async getMessageIds(threadId: string, limit: number): Promise<string[]> {
// async write(records: RecordWithTable[]): Promise<void> {
// async createAuthToken(token: AuthTokenRecord) {
// async createPassword(password: PasswordRecord) {
// async deleteAuthToken(authTokenId: string) {

// Tuple Index
// [table, id] -> record
//

const db: any = {} // KeyValueStore, OrderedKeyValueStore, ReducerTree
const query: any = {}

// All simple indexes... lets find a more challenging problem.
const indexes = {
	userByUsername: {
		match: [
			{
				user: {
					id: "id",
					username: "username",
				},
			},
		],
		sort: ["username", "id"],
	},
}

// explicit write checks.
const queries = {
	// getUserByUsername

	get: query(
		<T extends keyof Schema>(
			tx,
			table: T,
			id: string
		): Schema[T] | undefined => {
			return undefined
		}
	),
	// signup()
}

// API
// signup
// login
// logout
// changePassword
// createThread
// deleteThread
// inviteToThread
// editThread
// createMessage
// deleteMessage

// Permissions
