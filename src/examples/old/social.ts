// examples
// - social app
// - calendar app
// - notes app
// - end-user database app

import * as t from "./dataTypes"

const schema = {
	user: t.object({
		id: t.uuid,
		username: t.string,
	}),
	follow: t.object({
		id: t.tuple(t.uuid, t.uuid),
		created_at: t.datetime,
	}),
	post: t.object({
		id: t.uuid,
		created_at: t.datetime,
		author_id: t.uuid,
		text: t.string,
	}),
}

// user by username
// user's follows
// who's following me?
// timeline

const x = {
	userByUsername: {
		match: [{ user: { id: "id", username: "username" } }],
		sort: ["username", "id"],
	},
	followed: {
		match: [{ follow: { id: ["from", "to"] } }],
		sort: ["to", "form"],
	},
	secondOrderFollowing: {
		match: [{ follow: { id: ["A", "B"] } }, { follow: { id: ["B", "C"] } }],
		sort: ["A", "C", "B"],
	},
	secondOrderFollowed: {
		match: [{ follow: { id: ["A", "B"] } }, { follow: { id: ["B", "C"] } }],
		sort: ["C", "A", "B"],
	},
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
