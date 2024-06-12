// examples
// - messaging app
// - social app
// - calendar app
// - notes app
// - end-user database app

import * as t from "./dataTypes"

const shared = {
	id: t.uuid,
	version: t.number,
	last_version: t.optional(t.number),
	created_at: t.datetime,
	updated_at: t.datetime,
}

const schema = {
	user: t.object({
		...shared,
		username: t.string,
	}),
	/** password.id is same as user.id */
	password: t.object({
		...shared,
		password_hash: t.string,
	}),
	/** auth_token.id is the token */
	auth_token: t.object({
		...shared,
		user_id: t.uuid,
		expires_at: t.datetime,
	}),
	thread: t.object({
		...shared,
		created_by: t.uuid,
		member_ids: t.array(t.uuid),
		subject: t.string,
		deleted: t.optional(t.boolean),
	}),
	message: t.object({
		...shared,
		thread_id: t.uuid,
		author_id: t.uuid,
		text: t.string,
		file_ids: t.optional(t.array(t.uuid)),
		deleted: t.optional(t.boolean),
	}),
	file: t.object({
		...shared,
		owner_id: t.uuid,
		filename: t.string,
		deleted: t.optional(t.boolean),
	}),
}

type Schema = { [K in keyof typeof schema]: t.Infer<(typeof schema)[K]> }

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

// async getRecord<T extends RecordTable>(pointer: RecordPointer<T>) {
// async getRecords(pointers: RecordPointer[]): Promise<RecordMap> {
// async getUserByUsername(username: string) {
// async getPassword(userId: string) {
// async searchUsers(query: string) {
// async getThreadIds(userId: string, limit: number): Promise<string[]> {
// async getMessageIds(threadId: string, limit: number): Promise<string[]> {
// async write(records: RecordWithTable[]): Promise<void> {
// async createAuthToken(token: AuthTokenRecord) {
// async createPassword(password: PasswordRecord) {
// async deleteAuthToken(authTokenId: string) {
