/*

no latch crabbing.

TODO
- use async storage.
	- no in-place mutation!
	- concurrency and batch tests
	- storage tests.
- add a transaction helper

*/

import { RWLock } from "@ccorcos/lock"
import { orderedArray } from "@ccorcos/ordered-array"
import { AsyncKeyValueApi, KeyValueApi } from "./types"

type ReadTransactionApi<T> = {
	get: (key: string) => Promise<T | undefined>
}

export class ReadTransaction<T> implements ReadTransactionApi<T> {
	cache = new Map<string, T | undefined>()

	constructor(
		public storage: KeyValueApi<string, T> | AsyncKeyValueApi<string, T>
	) {}

	async get(key: string): Promise<T | undefined> {
		if (this.cache.has(key)) return this.cache.get(key)
		const value = await this.storage.get(key)
		this.cache.set(key, value)
		return value
	}
}

type ReadWriteTransactionApi<T> = {
	get: (key: string) => Promise<T | undefined>
	set: (key: string, value: T) => void
	delete: (key: string) => void
	commit: () => Promise<void>
}

export class ReadWriteTransaction<T>
	extends ReadTransaction<T>
	implements ReadWriteTransactionApi<T>
{
	sets = new Map<string, T>()
	deletes = new Set<string>()

	set(key: string, value: T) {
		// console.log("set:", key)
		this.sets.set(key, value)
		this.cache.set(key, value)
		this.deletes.delete(key)
	}

	delete(key: string) {
		// console.log("delete:", key)
		this.cache.set(key, undefined)
		this.sets.delete(key)
		this.deletes.add(key)
	}

	async commit() {
		const entries = Array.from(this.sets.entries())
		await this.storage.write({
			set: entries.map(([key, value]) => ({ key, value })),
			delete: Array.from(this.deletes),
		})
	}
}

export type BranchNode<K> = {
	leaf?: false
	id: string
	children: { minKey: K | null; childId: string }[]
}

export type LeafNode<K, V> = {
	leaf: true
	id: string
	values: { key: K; value: V }[]
}

function compare(a: any, b: any) {
	if (a === b) return 0
	if (a > b) return 1
	return -1
}

type NodeCursor<K, V> = {
	nodePath: (BranchNode<K> | LeafNode<K, V>)[]
	indexPath: number[]
}

export class AsyncBinaryPlusTree<K = string | number, V = any> {
	/**
	 * minSize must be less than maxSize / 2.
	 */
	constructor(
		public storage:
			| KeyValueApi<string, BranchNode<K> | LeafNode<K, V>>
			| AsyncKeyValueApi<string, BranchNode<K> | LeafNode<K, V>>,
		public minSize: number,
		public maxSize: number,
		public compareKey: (a: K, b: K) => number = compare
	) {
		if (minSize > maxSize / 2) throw new Error("Invalid tree size.")
	}

	lock = new RWLock()

	private leafValues = orderedArray(
		(item: { key: K }) => item.key,
		this.compareKey
	)

	private compareBranchKey = (a: K | null, b: K | null) => {
		if (a === null || b === null) {
			if (a === null) return -1
			if (b === null) return 1
		}
		return this.compareKey(a, b)
	}

	private branchChildren = orderedArray(
		(item: { minKey: K | null }) => item.minKey,
		this.compareBranchKey
	)

	private async findPath(
		tx: ReadTransactionApi<BranchNode<K> | LeafNode<K, V>>,
		key: K
	): Promise<NodeCursor<K, V>> {
		const nodePath: (BranchNode<K> | LeafNode<K, V>)[] = []
		const indexPath: number[] = []

		const root = await tx.get("root")
		if (!root) return { nodePath, indexPath }
		else nodePath.push(root)

		while (true) {
			const node = nodePath[0]
			if (node.leaf) return { nodePath, indexPath }

			const result = this.branchChildren.search(node.children, key)

			// Closest key that is at least as big as the key...
			// So the closest should never be less than the minKey.
			if (result.closest === 0) {
				console.log(node, key)
				throw new Error("Broken.")
			}

			const childIndex =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.children[childIndex].childId
			const child = await tx.get(childId)
			if (!child) throw Error("Missing child node.")
			nodePath.unshift(child)
			indexPath.unshift(childIndex)
		}
	}

	get = async (key: K): Promise<V | undefined> => {
		return this.lock.withRead(async () => {
			const tx = new ReadTransaction(this.storage)
			const { nodePath } = await this.findPath(tx, key)
			if (nodePath.length === 0) return

			const leaf = nodePath[0] as LeafNode<K, V>
			const result = this.leafValues.search(leaf.values, key)
			if (result.found === undefined) return
			return leaf.values[result.found].value
		})
	}

	private async startCursor(
		tx: ReadTransactionApi<BranchNode<K> | LeafNode<K, V>>
	) {
		const cursor: NodeCursor<K, V> = {
			nodePath: [],
			indexPath: [],
		}
		const root = await tx.get("root")
		if (!root) return cursor
		cursor.nodePath.push(root)

		while (true) {
			const node = cursor.nodePath[0]
			if (node.leaf) break
			const childIndex = 0
			const childId = node.children[childIndex].childId
			const child = await tx.get(childId)
			if (!child) throw new Error("Broken.")
			cursor.nodePath.unshift(child)
			cursor.indexPath.unshift(childIndex)
		}
		return cursor
	}

	private async nextCursor(
		tx: ReadTransactionApi<BranchNode<K> | LeafNode<K, V>>,
		cursor: NodeCursor<K, V>
	) {
		// console.log(cursor)
		cursor = {
			nodePath: [...cursor.nodePath],
			indexPath: [...cursor.indexPath],
		}
		for (let i = 0; i < cursor.nodePath.length - 1; i++) {
			// Find the point in the path where we need to go down a sibling branch.
			let parent = cursor.nodePath[i + 1] as BranchNode<K>
			const parentIndex = cursor.indexPath[i]
			const nextIndex = parentIndex + 1
			if (nextIndex >= parent.children.length) continue

			// Here's a branch.
			cursor.indexPath[i] = nextIndex

			// Fix the rest of the cursor.
			for (let j = i; j >= 0; j--) {
				let parent = cursor.nodePath[j + 1] as BranchNode<K>
				const parentIndex = cursor.indexPath[j]
				const childId = parent.children[parentIndex].childId
				const child = await tx.get(childId)
				if (!child) throw new Error("Broken.")
				cursor.nodePath[j] = child
				if (j > 0) cursor.indexPath[j - 1] = 0
			}
			return cursor
		}
	}

	private async endCursor(
		tx: ReadTransactionApi<BranchNode<K> | LeafNode<K, V>>
	) {
		const cursor: NodeCursor<K, V> = {
			nodePath: [],
			indexPath: [],
		}
		const root = await tx.get("root")
		if (!root) return cursor
		cursor.nodePath.push(root)
		while (true) {
			const node = cursor.nodePath[0]
			if (node.leaf) break
			const childIndex = node.children.length - 1
			const childId = node.children[childIndex].childId
			const child = await tx.get(childId)
			if (!child) throw new Error("Broken.")
			cursor.nodePath.unshift(child)
			cursor.indexPath.unshift(childIndex)
		}
		return cursor
	}

	private async prevCursor(
		tx: ReadTransactionApi<BranchNode<K> | LeafNode<K, V>>,
		cursor: NodeCursor<K, V>
	) {
		cursor = {
			nodePath: [...cursor.nodePath],
			indexPath: [...cursor.indexPath],
		}
		for (let i = 0; i < cursor.nodePath.length - 1; i++) {
			// Find the point in the path where we need to go down a sibling branch.
			const parentIndex = cursor.indexPath[i]
			const prevIndex = parentIndex - 1
			if (prevIndex < 0) continue

			// Here's a branch.
			cursor.indexPath[i] = prevIndex

			// Fix the rest of the cursor.
			for (let j = i; j >= 0; j--) {
				let parent = cursor.nodePath[j + 1] as BranchNode<K>
				const parentIndex = cursor.indexPath[j]
				const childId = parent.children[parentIndex].childId
				const child = await tx.get(childId)
				if (!child) throw new Error("Broken.")
				cursor.nodePath[j] = child
				if (j > 0)
					cursor.indexPath[j - 1] = child.leaf
						? child.values.length - 1
						: child.children.length - 1
			}
			return cursor
		}
	}

	list = async (
		args: {
			gt?: K
			gte?: K
			lt?: K
			lte?: K
			limit?: number
			reverse?: boolean
		} = {}
	) => {
		return this.lock.withRead(async () => {
			const tx = new ReadTransaction(this.storage)
			const results: { key: K; value: V }[] = []

			if (args.gt !== undefined && args.gte !== undefined)
				throw new Error("Invalid bounds: {gt, gte}")
			if (args.lt !== undefined && args.lte !== undefined)
				throw new Error("Invalid bounds: {lt, lte}")

			const start =
				args.gt !== undefined
					? args.gt
					: args.gte !== undefined
						? args.gte
						: undefined
			const startOpen = args.gt !== undefined
			const end =
				args.lt !== undefined
					? args.lt
					: args.lte !== undefined
						? args.lte
						: undefined
			const endOpen = args.lt !== undefined

			if (start !== undefined && end !== undefined) {
				const comp = this.compareKey(start, end)
				if (comp > 0) {
					console.warn("Invalid bounds.", args)
					return results
				}
				if (comp === 0 && (startOpen || endOpen)) {
					console.warn("Invalid bounds.", args)
					return results
				}
			}

			let startKey: NodeCursor<K, V> | undefined
			let endKey: NodeCursor<K, V> | undefined
			if (start !== undefined) {
				startKey = await this.findPath(tx, start)
			} else {
				startKey = await this.startCursor(tx)
			}
			if (end !== undefined) {
				endKey = await this.findPath(tx, end)
			} else {
				endKey = await this.endCursor(tx)
			}

			// No root node.
			if (startKey.nodePath.length === 0) return []

			if (args.reverse) {
				const leaf = endKey.nodePath[0] as LeafNode<K, V>
				if (end !== undefined) {
					const result = this.leafValues.search(leaf.values, end)
					const index =
						result.found !== undefined
							? endOpen
								? result.found
								: result.found + 1
							: result.closest
					results.push(...leaf.values.slice(0, index).reverse())
				} else {
					results.push(...leaf.values.slice(0).reverse())
				}

				// Start bound in the same leaf.
				if (
					start !== undefined &&
					this.compareKey(leaf.values[0].key, start) <= 0
				) {
					const result = this.leafValues.search(leaf.values, start)
					if (result.found !== undefined) {
						const startIndex = startOpen
							? results.length - result.found - 1
							: results.length - result.found
						results.splice(startIndex, results.length)
					} else {
						results.splice(results.length - result.closest, results.length)
					}

					// Start and limit bound
					if (args.limit && results.length >= args.limit) {
						results.splice(args.limit, results.length)
						return results
					}
					return results
				}

				// Limit bound
				if (args.limit && results.length >= args.limit) {
					results.splice(args.limit, results.length)
					return results
				}

				let cursor: NodeCursor<K, V> | undefined = endKey
				while ((cursor = await this.prevCursor(tx, cursor))) {
					const leaf = cursor.nodePath[0] as LeafNode<K, V>

					results.push(...leaf.values.slice(0).reverse())

					// Start bound
					if (
						start !== undefined &&
						this.compareKey(leaf.values[0].key, start) <= 0
					) {
						const result = this.leafValues.search(leaf.values, start)
						if (result.found !== undefined) {
							const startIndex = startOpen
								? results.length - result.found - 1
								: results.length - result.found
							results.splice(startIndex, results.length)
						} else {
							results.splice(results.length - result.closest, results.length)
						}

						// Start and limit bound
						if (args.limit && results.length >= args.limit) {
							results.splice(args.limit, results.length)
							return results
						}
						return results
					}

					// Limit bound
					if (args.limit && results.length >= args.limit) {
						results.splice(args.limit, results.length)
						return results
					}
				}

				return results
			}

			let startOffset = 0
			const leaf = startKey.nodePath[0] as LeafNode<K, V>
			if (start !== undefined) {
				const result = this.leafValues.search(leaf.values, start)
				const index =
					result.found !== undefined
						? startOpen
							? result.found + 1
							: result.found
						: result.closest
				startOffset = index
				results.push(...leaf.values.slice(index))
			} else {
				results.push(...leaf.values)
			}

			// End bound in the same leaf.
			if (
				end !== undefined &&
				this.compareKey(leaf.values[leaf.values.length - 1].key, end) >= 0
			) {
				const result = this.leafValues.search(leaf.values, end)

				if (result.found !== undefined) {
					const endIndex = endOpen
						? result.found - startOffset
						: result.found + 1 - startOffset
					results.splice(endIndex, results.length)
				} else {
					results.splice(result.closest - startOffset, results.length)
				}

				// End and limit bound
				if (args.limit && results.length >= args.limit) {
					results.splice(args.limit, results.length)
					return results
				}
				return results
			}

			// Limit bound
			if (args.limit && results.length >= args.limit) {
				results.splice(args.limit, results.length)
				return results
			}

			let cursor: NodeCursor<K, V> | undefined = startKey
			while ((cursor = await this.nextCursor(tx, cursor))) {
				const leaf = cursor.nodePath[0] as LeafNode<K, V>

				results.push(...leaf.values)

				// End bound
				if (
					end !== undefined &&
					this.compareKey(leaf.values[leaf.values.length - 1].key, end) >= 0
				) {
					const result = this.leafValues.search(results, end)
					if (result.found !== undefined) {
						const endIndex = endOpen ? result.found : result.found + 1
						results.splice(endIndex, results.length)
					} else {
						results.splice(result.closest, results.length)
					}

					// End and limit bound
					if (args.limit && results.length >= args.limit) {
						results.splice(args.limit, results.length)
						return results
					}
					return results
				}

				// Limit bound
				if (args.limit && results.length >= args.limit) {
					results.splice(args.limit, results.length)
					return results
				}
			}

			return results
		})
	}

	write = async (args: { set?: { key: K; value: V }[]; delete?: K[] }) => {
		return this.lock.withWrite(async () => {
			const tx = new ReadWriteTransaction(this.storage)
			for (const { key, value } of args.set || [])
				await this._set(tx, key, value)
			for (const key of args.delete || []) await this._delete(tx, key)
			await tx.commit()
		})
	}

	set = async (key: K, value: V) => {
		return this.lock.withWrite(async () => {
			const tx = new ReadWriteTransaction(this.storage)
			await this._set(tx, key, value)
			await tx.commit()
		})
	}

	delete = async (key: K) => {
		return this.lock.withWrite(async () => {
			const tx = new ReadWriteTransaction(this.storage)
			await this._delete(tx, key)
			await tx.commit()
		})
	}

	private async _set(
		tx: ReadWriteTransactionApi<BranchNode<K> | LeafNode<K, V>>,
		key: K,
		value: V
	) {
		const { nodePath, indexPath } = await this.findPath(tx, key)

		// Intitalize root node.
		if (nodePath.length === 0) {
			tx.set("root", {
				leaf: true,
				id: "root",
				values: [{ key, value }],
			})
			return
		}

		// Insert into leaf node.
		let leaf = nodePath[0] as LeafNode<K, V>
		leaf = { ...leaf, values: [...leaf.values] }
		const existing = this.leafValues.insert(leaf.values, { key, value })
		tx.set(leaf.id, leaf)
		nodePath[0] = leaf

		// No need to rebalance if we're replacing an existing item.
		if (existing) return

		// Balance the tree by splitting nodes, starting from the leaf.
		let node = nodePath.shift()
		while (node) {
			const size = node.leaf ? node.values.length : node.children.length
			if (size <= this.maxSize) break

			const splitIndex = Math.round(size / 2)

			if (node.leaf) {
				const rightNode: LeafNode<K, V> = {
					id: randomId(),
					leaf: true,
					values: node.values.slice(splitIndex),
				}
				tx.set(rightNode.id, rightNode)
				const rightMinKey = rightNode.values[0].key

				if (node.id === "root") {
					const leftNode: LeafNode<K, V> = {
						id: randomId(),
						leaf: true,
						values: node.values.slice(0, splitIndex),
					}
					tx.set(leftNode.id, leftNode)
					const rootNode: BranchNode<K> = {
						id: "root",
						leaf: false,
						children: [
							{ minKey: null, childId: leftNode.id },
							{ minKey: rightMinKey, childId: rightNode.id },
						],
					}
					tx.set("root", rootNode)
					break
				}

				// Insert right node into parent.
				node = { ...node, values: node.values.slice(0, splitIndex) }
				tx.set(node.id, node)

				let parent = nodePath.shift() as BranchNode<K>
				const parentIndex = indexPath.shift()
				if (!parent) throw new Error("Broken.")
				if (parentIndex === undefined) throw new Error("Broken.")

				parent = { ...parent, children: [...parent.children] }
				parent.children.splice(parentIndex + 1, 0, {
					minKey: rightMinKey,
					childId: rightNode.id,
				})
				tx.set(parent.id, parent)

				// Recur
				node = parent
				continue
			}

			const rightNode: BranchNode<K> = {
				id: randomId(),
				children: node.children.slice(splitIndex),
			}
			tx.set(rightNode.id, rightNode)
			const rightMinKey = rightNode.children[0].minKey

			if (node.id === "root") {
				const leftNode: BranchNode<K> = {
					id: randomId(),
					children: node.children.slice(0, splitIndex),
				}
				tx.set(leftNode.id, leftNode)
				const rootNode: BranchNode<K> = {
					id: "root",
					children: [
						{ minKey: null, childId: leftNode.id },
						{ minKey: rightMinKey, childId: rightNode.id },
					],
				}
				tx.set("root", rootNode)
				break
			}

			// Insert right node into parent.
			node = { ...node, children: node.children.slice(0, splitIndex) }
			tx.set(node.id, node)

			let parent = nodePath.shift() as BranchNode<K>
			const parentIndex = indexPath.shift()
			if (!parent) throw new Error("Broken.")
			if (parentIndex === undefined) throw new Error("Broken.")

			parent = { ...parent, children: [...parent.children] }
			parent.children.splice(parentIndex + 1, 0, {
				minKey: rightMinKey,
				childId: rightNode.id,
			})
			tx.set(parent.id, parent)

			// Recur
			node = parent
			continue
		}
	}

	private async _delete(
		tx: ReadWriteTransactionApi<BranchNode<K> | LeafNode<K, V>>,
		key: K
	) {
		const root = await tx.get("root")
		if (!root) return

		// Delete from leaf node.
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			let node = nodePath[0]

			if (node.leaf) {
				node = { ...node, values: [...node.values] }
				const exists = this.leafValues.remove(node.values, key)
				tx.set(node.id, node)
				if (!exists) return // No changes to the tree!

				// Continue to rebalance
				nodePath[0] = node
				break
			}

			// Recur into the child.
			const result = this.branchChildren.search(node.children, key)
			const index =
				result.found !== undefined ? result.found : result.closest - 1

			const childId = node.children[index].childId
			const child = await tx.get(childId)
			if (!child) throw Error("Missing child node.")
			nodePath.unshift(child)
			indexPath.unshift(index)
		}

		// Merge or redistribute to maintain minSize.
		let node = nodePath.shift()
		while (node) {
			if (node.id === "root") {
				// A root leaf node has no minSize constaint.
				if (node.leaf) return

				// Cleanup an empty root node.
				if (node.children.length === 0) {
					tx.delete("root")
					return
				}

				// A root node with one child becomes its child.
				if (node.children.length === 1) {
					const childId = node.children[0].childId
					const childNode = await tx.get(childId)
					if (!childNode) throw new Error("Broken.")
					tx.set("root", { ...childNode, id: "root" })
					tx.delete(childId)
				}
				return
			}

			let parent = nodePath.shift() as BranchNode<K>
			const parentIndex = indexPath.shift()
			if (!parent) throw new Error("Broken.")
			if (parentIndex === undefined) throw new Error("Broken.")

			const size = node.leaf ? node.values.length : node.children.length
			const minKey = node.leaf ? node.values[0].key : node.children[0].minKey

			// No need to merge but we might need to update the minKey in the parent
			if (size >= this.minSize) {
				const parentItem = parent.children[parentIndex]
				// No need to recusively update the left-most branch.
				if (parentItem.minKey === null) return
				// No need to recursively update if the minKey didn't change.
				if (this.compareBranchKey(parentItem.minKey, minKey) === 0) return
				// Set the minKey and recur
				parent = { ...parent, children: [...parent.children] }
				parent.children[parentIndex] = {
					minKey: minKey,
					childId: parentItem.childId,
				}
				tx.set(parent.id, parent)
				node = parent
				continue
			}

			// Merge or redistribute leaf nodes.
			if (node.leaf) {
				if (parentIndex === 0) {
					const rightId = parent.children[parentIndex + 1].childId
					let rightSibling = (await tx.get(rightId)) as typeof node
					if (!rightSibling) throw new Error("Broken.")

					const combinedSize = node.values.length + rightSibling.values.length

					// Redistribute leaf.
					if (combinedSize > this.maxSize) {
						const splitIndex = Math.round(combinedSize / 2) - node.values.length

						rightSibling = { ...rightSibling, values: [...rightSibling.values] }
						const moveLeft = rightSibling.values.splice(0, splitIndex)
						tx.set(rightSibling.id, rightSibling)

						node = { ...node, values: [...node.values] }
						node.values.push(...moveLeft)
						tx.set(node.id, node)

						// Update parent minKey.
						parent = { ...parent, children: [...parent.children] }

						if (parent.children[parentIndex].minKey !== null) {
							const leftMinKey = node.values[0].key
							parent.children[parentIndex] = {
								minKey: leftMinKey,
								childId: node.id,
							}
						}
						const rightMinKey = rightSibling.values[0].key
						parent.children[parentIndex + 1] = {
							minKey: rightMinKey,
							childId: rightSibling.id,
						}
						tx.set(parent.id, parent)

						// Recur
						node = parent
						continue
					}

					// Merge leaves.
					rightSibling = { ...rightSibling, values: [...rightSibling.values] }
					rightSibling.values.unshift(...node.values)

					// Remove the old pointer to rightSibling
					parent = { ...parent, children: [...parent.children] }
					parent.children.splice(1, 1)

					// Replace the node pointer with the new rightSibling
					const leftMost = parent.children[0].minKey === null
					parent.children[0] = {
						minKey: leftMost ? null : rightSibling.values[0].key,
						childId: rightSibling.id,
					}
					tx.set(rightSibling.id, rightSibling)
					tx.set(parent.id, parent)
					tx.delete(node.id)

					// Recur
					node = parent
					continue
				}

				// Merge or redistribute with left sibling.
				const leftId = parent.children[parentIndex - 1].childId
				let leftSibling = (await tx.get(leftId)) as typeof node
				if (!leftSibling) throw new Error("Broken.")

				const combinedSize = leftSibling.values.length + node.values.length

				// Redistribute leaf.
				if (combinedSize > this.maxSize) {
					const splitIndex = Math.round(combinedSize / 2)

					leftSibling = { ...leftSibling, values: [...leftSibling.values] }
					const moveRight = leftSibling.values.splice(splitIndex, this.maxSize)

					node = { ...node, values: [...node.values] }
					node.values.unshift(...moveRight)

					// Update parent keys.
					parent = { ...parent, children: [...parent.children] }
					parent.children[parentIndex] = {
						minKey: node.values[0].key,
						childId: node.id,
					}
					tx.set(leftSibling.id, leftSibling)
					tx.set(node.id, node)
					tx.set(parent.id, parent)

					// Recur
					node = parent
					continue
				}

				// Merge
				leftSibling = { ...leftSibling, values: [...leftSibling.values] }
				leftSibling.values.push(...node.values)

				// No need to update minKey because we added to the right.
				// Just need to delete the old node.
				parent = { ...parent, children: [...parent.children] }
				parent.children.splice(parentIndex, 1)

				tx.set(leftSibling.id, leftSibling)
				tx.set(parent.id, parent)
				tx.delete(node.id)

				// Recur
				node = parent
				continue
			}

			// Merge or redistribute branch nodes.
			if (parentIndex === 0) {
				const rightId = parent.children[parentIndex + 1].childId
				let rightSibling = (await tx.get(rightId)) as typeof node
				if (!rightSibling) throw new Error("Broken.")

				const combinedSize = node.children.length + rightSibling.children.length

				// Redistribute leaf.
				if (combinedSize > this.maxSize) {
					const splitIndex = Math.round(combinedSize / 2) - node.children.length

					rightSibling = {
						...rightSibling,
						children: [...rightSibling.children],
					}
					const moveLeft = rightSibling.children.splice(0, splitIndex)
					tx.set(rightSibling.id, rightSibling)

					node = { ...node, children: [...node.children] }
					node.children.push(...moveLeft)
					tx.set(node.id, node)

					// Update parent minKey.
					parent = { ...parent, children: [...parent.children] }
					if (parent.children[parentIndex].minKey !== null) {
						const leftMinKey = node.children[0].minKey
						parent.children[parentIndex] = {
							minKey: leftMinKey,
							childId: node.id,
						}
					}
					const rightMinKey = rightSibling.children[0].minKey
					parent.children[parentIndex + 1] = {
						minKey: rightMinKey,
						childId: rightSibling.id,
					}
					tx.set(parent.id, parent)

					// Recur
					node = parent
					continue
				}

				// Merge leaves.
				rightSibling = { ...rightSibling, children: [...rightSibling.children] }
				rightSibling.children.unshift(...node.children)

				// Remove the old pointer to rightSibling
				parent = { ...parent, children: [...parent.children] }
				parent.children.splice(1, 1)

				// Replace the node pointer with the new rightSibling
				const leftMost = parent.children[0].minKey === null
				parent.children[0] = {
					minKey: leftMost ? null : rightSibling.children[0].minKey,
					childId: rightSibling.id,
				}
				tx.set(rightSibling.id, rightSibling)
				tx.set(parent.id, parent)
				tx.delete(node.id)

				// Recur
				node = parent
				continue
			}

			// Merge or redistribute with left sibling.
			const leftId = parent.children[parentIndex - 1].childId
			let leftSibling = (await tx.get(leftId)) as typeof node
			if (!leftSibling) throw new Error("Broken.")

			const combinedSize = leftSibling.children.length + node.children.length

			// Redistribute branch.
			if (combinedSize > this.maxSize) {
				const splitIndex = Math.round(combinedSize / 2)

				leftSibling = { ...leftSibling, children: [...leftSibling.children] }
				const moveRight = leftSibling.children.splice(splitIndex, this.maxSize)

				node = { ...node, children: [...node.children] }
				node.children.unshift(...moveRight)

				// Update parent keys.
				parent = { ...parent, children: [...parent.children] }
				parent.children[parentIndex] = {
					minKey: node.children[0].minKey,
					childId: node.id,
				}
				tx.set(leftSibling.id, leftSibling)
				tx.set(node.id, node)
				tx.set(parent.id, parent)

				// Recur
				node = parent
				continue
			}

			// Merge
			leftSibling = { ...leftSibling, children: [...leftSibling.children] }
			leftSibling.children.push(...node.children)

			// No need to update minKey because we added to the right.
			// Just need to delete the old node.
			parent = { ...parent, children: [...parent.children] }
			parent.children.splice(parentIndex, 1)

			tx.set(leftSibling.id, leftSibling)
			tx.set(parent.id, parent)
			tx.delete(node.id)

			// Recur
			node = parent
			continue
		}
	}
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}
