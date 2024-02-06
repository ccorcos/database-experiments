/*

B+ tree with latch crabbing locking
https://stackoverflow.com/questions/52058099/making-a-btree-concurrent-c

better types, jsonCodec, and list query.

*/

import { orderedArray } from "@ccorcos/ordered-array"

import { RWLockMap } from "@ccorcos/lock"

export type KeyValueStorage<K = string, V = any> = {
	get: (key: K) => Promise<V | undefined>
	write: (tx: { set?: { key: K; value: V }[]; delete?: K[] }) => Promise<void>
}

export class KeyValueDatabase<T = any> {
	constructor(public storage: KeyValueStorage<string, T>) {}

	async get(key: string) {
		return await this.storage.get(key)
	}

	async write(tx: { set?: { key: string; value: T }[]; delete?: string[] }) {
		await this.storage.write(tx)
	}

	locks = new RWLockMap()

	transact() {
		return new KeyValueTransaction(this)
	}
}

export class KeyValueTransaction<T> {
	locks = new Set<() => void>()
	cache: { [key: string]: T | undefined } = {}
	sets: { [key: string]: T } = {}
	deletes = new Set<string>()

	constructor(public kv: KeyValueDatabase<T>) {}

	async readLock(key: string) {
		// console.log("READ", key)
		const release = await this.kv.locks.read(key)
		this.locks.add(release)
		return () => {
			this.locks.delete(release)
			release()
		}
	}

	async writeLock(key: string) {
		// console.trace("WRITE", key)
		const release = await this.kv.locks.write(key)
		this.locks.add(release)
		return () => {
			this.locks.delete(release)
			release()
		}
	}

	async get(key: string): Promise<T | undefined> {
		if (key in this.cache) return this.cache[key]
		const value = await this.kv.get(key)
		this.cache[key] = value
		return value
	}

	set(key: string, value: T) {
		this.sets[key] = value
		this.cache[key] = value
		this.deletes.delete(key)
	}

	delete(key: string) {
		this.cache[key] = undefined
		delete this.sets[key]
		this.deletes.add(key)
	}

	release() {
		for (const release of this.locks) release()
	}

	async commit() {
		await this.kv.write({
			set: Object.entries(this.sets).map(([key, value]) => ({ key, value })),
			delete: Array.from(this.deletes),
		})
		this.release()
	}
}

type Key = string | number

/**
 * id references the node in a key-value database.
 * Each item in values has a `key` that is the minKey of the child node with id `value`.
 * The key will be null for the left-most branch nodes.
 */
export type BranchNode = {
	leaf?: false
	id: string
	values: { key: Key | null; value: string }[]
}

export type LeafNode = {
	leaf: true
	id: string
	values: { key: Key | null; value: any }[]
}

const { search, insert, remove } = orderedArray(
	(item: { key: Key | null }) => item.key,
	(a, b) => {
		if (a === b) return 0
		if (a === null) return -1
		if (b === null) return 1
		if (a > b) return 1
		else return -1
	}
)

export class AsyncBinaryPlusTree {
	/**
	 * minSize must be less than maxSize / 2.
	 */
	constructor(
		storage: KeyValueStorage<string, BranchNode | LeafNode>,
		public minSize: number,
		public maxSize: number
	) {
		if (minSize > maxSize / 2) throw new Error("Invalid tree size.")
		this.kv = new KeyValueDatabase(storage)
	}

	kv: KeyValueDatabase

	locks = new RWLockMap()

	// Commit transaction for read-concurrency checks.
	async get(key: Key): Promise<any | undefined> {
		const tx = this.kv.transact()

		let releaseNode = await tx.readLock("root")
		const root = await tx.get("root")
		if (!root) {
			tx.release()
			return // Empty tree
		}

		let node = root
		while (true) {
			if (node.leaf) {
				const result = search(node.values, key)
				if (result.found === undefined) {
					tx.release()
					return
				}
				tx.release()
				return node.values[result.found].value
			}

			const result = search(node.values, key)

			// Closest key that is at least as big as the key...
			// So the closest should never be less than the minKey.
			if (result.closest === 0) {
				tx.release()
				throw new Error("Broken.")
			}

			const childIndex =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.values[childIndex].value

			// Latch Crabbing
			const releaseChild = await tx.readLock(childId)
			const child = await tx.get(childId)
			if (!child) {
				tx.release()
				throw Error("Missing child node.")
			}

			releaseNode()
			node = child
			releaseNode = releaseChild
			continue
		}
	}

	async set(key: Key, value: any) {
		const tx = this.kv.transact()

		let releaseNode = await tx.writeLock("root")
		const root = await tx.get("root")

		// Intitalize root node.
		if (!root) {
			tx.set("root", {
				leaf: true,
				id: "root",
				values: [{ key, value }],
			})
			await tx.commit()
			return
		}

		// Insert into leaf node.
		let releaseAncestors = () => {}
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			const node = nodePath[0]

			// Latch Crabbing
			if (node.values.length < this.maxSize) {
				releaseAncestors()
			}

			if (node.leaf) {
				const newNode = { ...node, values: [...node.values] }
				const existing = insert(newNode.values, { key, value })
				tx.set(newNode.id, newNode)

				// No need to rebalance if we're replacing
				if (existing) {
					await tx.commit()
					return
				}

				// Replace the node and balance the tree.
				nodePath[0] = newNode
				break
			}

			const result = search(node.values, key)
			const childIndex =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.values[childIndex].value

			const releaseChild = await tx.writeLock(childId)
			const child = await tx.get(childId)
			if (!child) {
				tx.release()
				throw Error("Missing child node.")
			}

			// Latch Crabbing
			releaseAncestors = combine(releaseNode, releaseAncestors)
			releaseNode = releaseChild

			// Recur into child.
			nodePath.unshift(child)
			indexPath.unshift(childIndex)
		}

		// Balance the tree by splitting nodes, starting from the leaf.
		let node = nodePath.shift()
		while (node) {
			const size = node.values.length
			if (size <= this.maxSize) {
				await tx.commit()
				return
			}

			const splitIndex = Math.round(size / 2)
			const rightNode: LeafNode | BranchNode = {
				id: randomId(),
				leaf: node.leaf,
				values: node.values.splice(splitIndex),
			}
			tx.set(rightNode.id, rightNode)
			const rightMinKey = rightNode.values[0].key

			// If we're splitting the root node, we want to keep the root id.
			if (node.id === "root") {
				const leftNode: LeafNode | BranchNode = {
					id: randomId(),
					leaf: node.leaf,
					values: node.values,
				}
				tx.set(leftNode.id, leftNode)

				const newRoot: LeafNode | BranchNode = {
					id: "root",
					values: [
						{ key: null, value: leftNode.id },
						{ key: rightMinKey, value: rightNode.id },
					],
				}
				tx.set(newRoot.id, newRoot)
				await tx.commit()
				return
			}

			// Insert right node into parent.
			const parent = nodePath.shift()
			const parentIndex = indexPath.shift()
			if (!parent) {
				tx.release()
				throw new Error("Broken.")
			}
			if (parentIndex === undefined) {
				tx.release()
				throw new Error("Broken.")
			}

			const newParent = { ...parent, values: [...parent.values] }
			newParent.values.splice(parentIndex + 1, 0, {
				key: rightMinKey,
				value: rightNode.id,
			})
			tx.set(newParent.id, newParent)

			// Recur
			node = newParent
		}

		throw new Error("Broken.")
	}

	async delete(key: Key) {
		const tx = this.kv.transact()

		let releaseNode = await tx.writeLock("root")
		const root = await tx.get("root")
		if (!root) {
			tx.release()
			return
		}

		// Delete from leaf node.
		let releaseAncestors = () => {}
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			const node = nodePath[0]

			// Latch Crabbing, if we on't need to merge, or update parentKey.
			if (node.values.length > this.minSize && node.values[0].key !== key) {
				releaseAncestors()
			}

			if (node.leaf) {
				const newNode = { ...node, values: [...node.values] }
				const exists = remove(newNode.values, key)
				tx.set(newNode.id, newNode)
				if (!exists) {
					await tx.commit()
					return
				}
				// Continue to rebalance.
				nodePath[0] = newNode
				break
			}

			const result = search(node.values, key)
			const index =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.values[index].value
			const releaseChild = await tx.writeLock(childId)
			const child = await tx.get(childId)
			if (!child) {
				tx.release()
				throw Error("Missing child node.")
			}

			// Latch Crabbing
			releaseAncestors = combine(releaseNode, releaseAncestors)
			releaseNode = releaseChild

			// Recur into the child.
			nodePath.unshift(child)
			indexPath.unshift(index)
		}

		/*

		Step-by-step explanation of the more complicated case.

		Imagine a tree with minSize = 2, maxSize = 4.

		[null,10]
		[null,5] [10,15]
		[2,4] [5,7] [10,11] [15,24]

		Removing 10 from the leaf

		[null,10]
		[null,5] [10,15]
		[2,4] [5,7] [11] [15,24]

		Loop: Merge and update parent pointers.

		[null,10]
		[null,5] [11]
		[2,4] [5,7] [11,15,24]

		Recurse into parent.

		[null]
		[null,5,11]
		[2,4] [5,7] [11,15,24]

		Replace the root with child if there is only one key

		[null,5,11]
		[2,4] [5,7] [11,15,24]

		*/

		let node = nodePath.shift()
		while (node) {
			if (node.id === "root") {
				// A root leaf node has no minSize constaint.
				if (node.leaf) {
					await tx.commit()
					return
				}

				// Root node with only one child becomes its child.
				if (node.values.length === 1) {
					const childId = node.values[0].value
					// No need to lock here because it should already be locked.
					const child = await tx.get(childId)
					if (!child) {
						tx.release()
						throw new Error("Broken.")
					}
					const newRoot = { ...child, id: "root" }
					tx.set(newRoot.id, newRoot)
					tx.delete(childId)
				}

				await tx.commit()
				return
			}

			const parent = nodePath.shift()
			const parentIndex = indexPath.shift()
			if (!parent) {
				tx.release()
				throw new Error("Broken.")
			}
			if (parentIndex === undefined) {
				tx.release()
				throw new Error("Broken.")
			}

			if (node.values.length >= this.minSize) {
				// No need to merge but we might need to update the minKey in the parent
				const parentItem = parent.values[parentIndex]
				// No need to recusively update the left-most branch.
				if (parentItem.key === null) {
					await tx.commit()
					return
				}
				// No need to recursively update if the minKey didn't change.
				if (parentItem.key === node.values[0].key) {
					await tx.commit()
					return
				}

				// Set the minKey and recur
				const newParent = { ...parent, values: [...parent.values] }
				newParent.values[parentIndex] = {
					key: node.values[0].key,
					value: parentItem.value,
				}
				tx.set(newParent.id, newParent)
				node = newParent
				continue
			}

			// Merge or redistribute
			if (parentIndex === 0) {
				// When we delete from the first element, merge/redistribute with right sibling.
				const rightId = parent.values[parentIndex + 1].value
				const releaseRight = await tx.writeLock(rightId)
				const rightSibling = await tx.get(rightId)
				if (!rightSibling) {
					tx.release()
					throw new Error("Broken.")
				}

				const combinedSize = node.values.length + rightSibling.values.length
				if (combinedSize > this.maxSize) {
					// Redistribute between both nodes.
					const splitIndex = Math.round(combinedSize / 2) - node.values.length

					const newRight = { ...rightSibling, values: [...rightSibling.values] }
					const moveLeft = newRight.values.splice(0, splitIndex)
					tx.set(newRight.id, newRight)

					const newNode = { ...node, values: [...node.values] }
					newNode.values.push(...moveLeft)
					tx.set(newNode.id, newNode)

					// Update parent minKey.
					const newParent = { ...parent, values: [...parent.values] }
					if (parent.values[parentIndex].key !== null) {
						newParent.values[parentIndex] = {
							key: newNode.values[0].key,
							value: newParent.values[parentIndex].value,
						}
					}

					newParent.values[parentIndex + 1] = {
						key: newRight.values[0].key,
						value: newParent.values[parentIndex + 1].value,
					}
					tx.set(newParent.id, newParent)

					// Recur
					node = newParent
					continue
				}

				// Merge
				const newRight = { ...rightSibling, values: [...rightSibling.values] }
				newRight.values.unshift(...node.values)

				// Remove the old pointer to rightSibling
				const newParent = { ...parent, values: [...parent.values] }
				newParent.values.splice(1, 1)

				// Replace the node pointer with the new rightSibling
				const leftMost = newParent.values[0].key === null
				newParent.values[0] = {
					key: leftMost ? null : newRight.values[0].key,
					value: newRight.id,
				}
				tx.set(newRight.id, newRight)
				tx.set(newParent.id, newParent)
				tx.delete(node.id)

				// Recur
				node = newParent
				continue
			}

			// Merge/redistribute with left sibling.
			const leftId = parent.values[parentIndex - 1].value
			const releaseLeft = await tx.writeLock(leftId)
			const leftSibling = await tx.get(leftId)
			if (!leftSibling) {
				tx.release()
				throw new Error("Broken.")
			}

			const combinedSize = leftSibling.values.length + node.values.length
			if (combinedSize > this.maxSize) {
				// Redistribute
				const splitIndex = Math.round(combinedSize / 2)

				const newLeft = { ...leftSibling, values: [...leftSibling.values] }
				const moveRight = newLeft.values.splice(splitIndex, this.maxSize)

				const newNode = { ...node, values: [...node.values] }
				newNode.values.unshift(...moveRight)

				// Update parent keys.
				const newParent = { ...parent, values: [...parent.values] }
				newParent.values[parentIndex] = {
					key: newNode.values[0].key,
					value: newParent.values[parentIndex].value,
				}
				tx.set(newLeft.id, newLeft)
				tx.set(newNode.id, newNode)
				tx.set(newParent.id, newParent)

				// Recur
				node = newParent
				continue
			}

			// Merge
			const newLeft = { ...leftSibling, values: [...leftSibling.values] }
			newLeft.values.push(...node.values)

			// No need to update minKey because we added to the right.
			// Just need to delete the old node.
			const newParent = { ...parent, values: [...parent.values] }
			newParent.values.splice(parentIndex, 1)

			tx.set(newLeft.id, newLeft)
			tx.set(newParent.id, newParent)
			tx.delete(node.id)

			// Recur
			node = newParent
			continue
		}

		tx.release()
	}

	async depth() {
		const tx = this.kv.transact()
		let releaseNode = await tx.readLock("root")
		const root = await tx.get("root")
		if (!root) {
			tx.release()
			return 0
		}
		let depth = 1
		let node = root
		while (!node.leaf) {
			depth += 1
			const childId = node.values[0].value
			const releaseChld = await tx.readLock(childId)
			releaseNode()
			releaseNode = releaseChld
			const nextNode = await tx.get(childId)
			if (!nextNode) {
				tx.release()
				throw new Error("Broken.")
			}
			node = nextNode
		}
		tx.release()
		return depth
	}
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}

function combine(fn1: () => void, fn2: () => void) {
	return () => {
		fn1()
		fn2()
	}
}
