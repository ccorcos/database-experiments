/*

Both Postgres and SQLite use B+ trees as the foundation of their indexes.

Even though we have an OrderedKeyValueDatabase, let's build a B+ tree on top of a KeyValueDatabase
so that we can later extend it to an interval tree and a range tree.

*/

import { orderedArray } from "@ccorcos/ordered-array"
import { KeyValueDatabase } from "./kv"

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
	(item: { key: Key | null; value: string }) => item.key
)

export class BinaryPlusKeyValueDatabase {
	/**
	 * minSize must be less than maxSize / 2.
	 */
	constructor(
		public kv: KeyValueDatabase<BranchNode | LeafNode>,
		public minSize: number,
		public maxSize: number
	) {
		if (minSize > maxSize / 2) throw new Error("Invalid tree size.")
	}

	// Commit transaction for read-concurrency checks.
	get = (key: Key): any | undefined => {
		const tx = new KeyValueTransaction(this.kv)

		const root = tx.get("root")
		if (!root) {
			// No need to tx.check(), we only read one value.
			return // Empty tree
		}

		let node = root
		while (true) {
			if (node.leaf) {
				const result = search(node.values, key)
				if (result.found === undefined) {
					if (node.id !== "root") tx.check()
					return
				}
				if (node.id !== "root") tx.check()
				return node.values[result.found].value
			}

			const result = search(node.values, key)

			// Closest key that is at least as big as the key...
			// So the closest should never be less than the minKey.
			if (result.closest === 0) {
				tx.check()
				throw new Error("Broken.")
			}

			const childIndex =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.values[childIndex].value
			const child = tx.get(childId)
			if (!child) {
				// Check first in case this node was deleted based on a concurrent write.
				tx.check()
				throw Error("Missing child node.")
			}
			node = child
			continue
		}
	}

	set = (key: Key, value: any) => {
		const tx = new KeyValueTransaction(this.kv)
		const root = tx.get("root")

		// Intitalize root node.
		if (!root) {
			tx.set("root", {
				leaf: true,
				id: "root",
				values: [{ key, value }],
			})
			tx.commit()
			return
		}

		// Insert into leaf node.
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			const node = nodePath[0]

			if (node.leaf) {
				const newNode = { ...node, values: [...node.values] }
				const existing = insert(newNode.values, { key, value })
				tx.set(newNode.id, newNode)

				// No need to rebalance if we're replacing
				if (existing) {
					tx.commit()
					return
				}

				// Replace the node and balance the tree.
				nodePath[0] = newNode
				break
			}

			const result = search(node.values, key)
			const index =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.values[index].value
			const child = tx.get(childId)
			if (!child) {
				tx.check()
				throw Error("Missing child node.")
			}
			// Recur into child.
			nodePath.unshift(child)
			indexPath.unshift(index)
		}

		// Balance the tree by splitting nodes, starting from the leaf.
		let node = nodePath.shift()
		while (node) {
			const size = node.values.length
			if (size <= this.maxSize) {
				tx.commit()
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
				tx.commit()
				return
			}

			// Insert right node into parent.
			const parent = nodePath.shift()
			const parentIndex = indexPath.shift()
			if (!parent) {
				tx.check()
				throw new Error("Broken.")
			}
			if (parentIndex === undefined) {
				tx.check()
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
	}

	delete = (key: Key) => {
		const tx = new KeyValueTransaction(this.kv)
		const root = tx.get("root")
		if (!root) {
			// No need to tx.check()
			return
		}

		// Delete from leaf node.
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			const node = nodePath[0]

			if (node.leaf) {
				const newNode = { ...node, values: [...node.values] }
				const exists = remove(newNode.values, key)
				tx.set(newNode.id, newNode)
				if (!exists) {
					tx.commit()
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
			const child = tx.get(childId)
			if (!child) {
				tx.check()
				throw Error("Missing child node.")
			}

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
					tx.commit()
					return
				}

				// Root node with only one child becomes its child.
				if (node.values.length === 1) {
					const childId = node.values[0].value
					const child = tx.get(childId)
					if (!child) {
						tx.check()
						throw new Error("Broken.")
					}
					const newRoot = { ...child, id: "root" }
					tx.set(newRoot.id, newRoot)
				}

				tx.commit()
				return
			}

			const parent = nodePath.shift()
			const parentIndex = indexPath.shift()
			if (!parent) {
				tx.check()
				throw new Error("Broken.")
			}
			if (parentIndex === undefined) {
				tx.check()
				throw new Error("Broken.")
			}

			if (node.values.length >= this.minSize) {
				// No need to merge but we might need to update the minKey in the parent
				const parentItem = parent.values[parentIndex]
				// No need to recusively update the left-most branch.
				if (parentItem.key === null) {
					tx.commit()
					return
				}
				// No need to recursively update if the minKey didn't change.
				if (parentItem.key === node.values[0].key) {
					tx.commit()
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
				const rightSibling = tx.get(rightId)
				if (!rightSibling) {
					tx.check()
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

				// Recur
				node = newParent
				continue
			}

			// Merge/redistribute with left sibling.
			const leftId = parent.values[parentIndex - 1].value
			const leftSibling = tx.get(leftId)
			if (!leftSibling) {
				tx.check()
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

			// Recur
			node = newParent
			continue
		}
	}

	depth() {
		const tx = new KeyValueTransaction(this.kv)
		const root = tx.get("root")
		if (!root) return 0
		let depth = 1
		let node = root
		while (!node.leaf) {
			depth += 1
			const nextNode = tx.get(node.values[0].value)
			if (!nextNode) {
				tx.check()
				throw new Error("Broken.")
			}
			node = nextNode
		}
		tx.check()
		return depth
	}
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}

export class KeyValueTransaction {
	checks: { [key: string]: string | undefined } = {}
	cache: { [key: string]: BranchNode | LeafNode | undefined } = {}
	sets: { [key: string]: BranchNode | LeafNode } = {}
	deletes = new Set<string>()

	constructor(private kv: KeyValueDatabase<BranchNode | LeafNode>) {}

	get = (key: string): BranchNode | LeafNode | undefined => {
		if (key in this.cache) return this.cache[key]
		const result = this.kv.get(key)
		this.checks[key] = result?.version
		this.cache[key] = result?.value
		return result?.value
	}

	set(key: string, value: BranchNode | LeafNode) {
		this.sets[key] = value
		this.cache[key] = value
		this.deletes.delete(key)
	}

	delete(key: string) {
		this.cache[key] = undefined
		delete this.sets[key]
		this.deletes.add(key)
	}

	check() {
		this.kv.write({
			check: Object.entries(this.checks).map(([key, version]) => ({
				key,
				version,
			})),
		})
	}

	commit() {
		this.kv.write({
			check: Object.entries(this.checks).map(([key, version]) => ({
				key,
				version,
			})),
			set: Object.entries(this.sets).map(([key, value]) => ({ key, value })),
			delete: Array.from(this.deletes),
		})
	}
}
