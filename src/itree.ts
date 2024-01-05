import { orderedArray } from "@ccorcos/ordered-array"
import { ArrayEncoding } from "lexicodec"

type Value = string | number

// Represents a primary key. We're still storing in [min, max, id] order.
type Key = { min: Value; max: Value; key: Value }

// null represents -Infinity bound.
function compareValue(a: Value | null, b: Value | null) {
	if (a === b) return 0
	if (a === null) return -1
	if (b === null) return 1
	if (a > b) return 1
	else return -1
}

// Compare key as a tuple.
// null represents -Infinity bound.
function compareKey(a: Key | null, b: Key | null) {
	if (a === b) return 0
	if (a === null) return -1
	if (b === null) return 1
	return ArrayEncoding.compare(
		[a.min, a.max, a.key],
		[b.min, b.max, b.key],
		compareValue
	)
}

export type BranchNode = {
	leaf?: false
	id: string
	rangeMax: Value
	values: {
		minKey: null | Key
		rangeMax: Value
		id: string // nodeId
	}[]
}

export type LeafNode = {
	leaf: true
	id: string
	rangeMax: Value
	values: {
		key: Key
		value: any
	}[]
}

const { insert: insertLeaf } = orderedArray(
	(item: { key: Key }) => item.key,
	compareKey
)

const { search: searchBranch } = orderedArray(
	(item: { minKey: Key | null }) => item.minKey,
	compareKey
)

function maxKey(a: Key, b: Key) {
	return compareKey(a, b) >= 0 ? a : b
}

function intersets(
	a: { min: Value | null; max: Value },
	b: { min: Value | null; max: Value }
) {
	return compareValue(a.min, b.max) <= 0 && compareValue(a.max, b.min) >= 0
}

export class IntervalTree {
	// In preparation for storing nodes in a key-value database.
	nodes: { [key: Value]: BranchNode | LeafNode | undefined } = {}

	/**
	 * minSize must be less than maxSize / 2.
	 */
	constructor(public minSize: number, public maxSize: number) {
		if (minSize > maxSize / 2) throw new Error("Invalid tree size.")
	}

	get(queryRange: { min: Value; max: Value }): any[] {
		const root = this.nodes["root"]
		if (!root) return [] // Empty tree

		const results: any[] = []
		const cursor = [root]
		while (cursor.length > 0) {
			const node = cursor.shift()!
			if (node.leaf) {
				for (const item of node.values) {
					if (intersets(queryRange, item.key)) {
						results.push(item)
					}
				}
				continue
			}

			for (const item of node.values) {
				const itemRange = {
					min: item.minKey === null ? null : item.minKey[0],
					max: item.rangeMax,
				}

				if (intersets(queryRange, itemRange)) {
					const childId = item.id
					const child = this.nodes[childId]
					if (!child) throw Error("Missing child node.")
					cursor.push(child)
				}
			}
		}
		return results
	}

	set(key: { min: Value; max: Value; key: Value }, value: any) {
		const root = this.nodes["root"]

		// Intitalize root node.
		if (!root) {
			this.nodes["root"] = {
				leaf: true,
				id: "root",
				rangeMax: key.max,
				values: [{ key, value }],
			}
			return
		}

		// Insert into leaf node.
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			const node = nodePath[0]

			if (node.leaf) {
				insertLeaf(node.values, { key, value })
				break
			}

			const result = searchBranch(node.values, key)
			const index =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.values[index].id
			const child = this.nodes[childId]
			if (!child) throw Error("Missing child node.")
			// Recur into child.
			nodePath.unshift(child)
			indexPath.unshift(index)
		}

		// Balance the tree by splitting nodes, starting from the leaf.
		// Also update the rangeMax values going up to the root.
		let node = nodePath.shift()
		while (node) {
			if (node.id === "root") {
				// Update rangeMax.
				if (compareValue(key.max, node.rangeMax) > 0) {
					node.rangeMax = key.max
				}

				const size = node.values.length
				if (size > this.maxSize) {
					const splitIndex = Math.round(size / 2)

					const rightNode: LeafNode | BranchNode = {
						id: randomId(),
						leaf: node.leaf,
						values: node.values.splice(splitIndex),
					}
					this.nodes[rightNode.id] = rightNode
					const rightMinKey = rightNode.values[0].key

					const leftNode: LeafNode | BranchNode = {
						id: randomId(),
						leaf: node.leaf,
						values: node.values,
					}
					this.nodes[leftNode.id] = leftNode

					this.nodes["root"] = {
						id: "root",
						values: [
							{ key: null, value: leftNode.id },
							{ key: rightMinKey, value: rightNode.id },
						],
					}
				}
				break
			}

			const size = node.values.length
			if (size <= this.maxSize) break

			const splitIndex = Math.round(size / 2)
			const rightNode: LeafNode | BranchNode = {
				id: randomId(),
				leaf: node.leaf,
				values: node.values.splice(splitIndex),
			}
			this.nodes[rightNode.id] = rightNode
			const rightMinKey = rightNode.values[0].key

			// If we're splitting the root node.
			if (node.id === "root") {
				const leftNode: LeafNode | BranchNode = {
					id: randomId(),
					leaf: node.leaf,
					values: node.values,
				}
				this.nodes[leftNode.id] = leftNode

				this.nodes["root"] = {
					id: "root",
					values: [
						{ key: null, value: leftNode.id },
						{ key: rightMinKey, value: rightNode.id },
					],
				}
				break
			}

			// Insert right node into parent.
			const parent = nodePath.shift()
			const parentIndex = indexPath.shift()
			if (!parent) throw new Error("Broken.")
			if (parentIndex === undefined) throw new Error("Broken.")

			parent.values.splice(parentIndex + 1, 0, {
				key: rightMinKey,
				value: rightNode.id,
			})

			// Recur
			node = parent
		}
	}

	delete = (key: Value) => {
		const root = this.nodes["root"]
		if (!root) return

		// Delete from leaf node.
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			const node = nodePath[0]

			if (node.leaf) {
				const exists = remove(node.values, key)
				if (!exists) return
				break
			}

			const result = search(node.values, key)
			const index =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.values[index].value
			const child = this.nodes[childId]
			if (!child) throw Error("Missing child node.")
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
				if (node.leaf) return

				// If node with only one child becomes its child.
				if (node.values.length === 1) {
					const childId = node.values[0].value
					const childNode = this.nodes[childId]
					if (!childNode) throw new Error("Broken.")
					this.nodes["root"] = { ...childNode, id: "root" }
					delete this.nodes[childId]
				}
				return
			}

			const parent = nodePath.shift()
			const parentIndex = indexPath.shift()
			if (!parent) throw new Error("Broken.")
			if (parentIndex === undefined) throw new Error("Broken.")

			if (node.values.length >= this.minSize) {
				// No need to merge but we might need to update the minKey in the parent
				const parentItem = parent.values[parentIndex]
				// No need to recusively update the left-most branch.
				if (parentItem.key === null) return
				// No need to recursively update if the minKey didn't change.
				if (parentItem.key === node.values[0].key) return
				// Set the minKey and recur
				parentItem.key = node.values[0].key
				node = parent
				continue
			}

			// Merge or redistribute
			if (parentIndex === 0) {
				const rightSibling = this.nodes[parent.values[parentIndex + 1].value]
				if (!rightSibling) throw new Error("Broken.")

				const combinedSize = node.values.length + rightSibling.values.length
				if (combinedSize > this.maxSize) {
					// Redistribute
					const splitIndex = Math.round(combinedSize / 2) - node.values.length
					const moveLeft = rightSibling.values.splice(0, splitIndex)
					node.values.push(...moveLeft)

					// Update parent keys.
					if (parent.values[parentIndex].key !== null) {
						parent.values[parentIndex].key = node.values[0].key
					}
					parent.values[parentIndex + 1].key = rightSibling.values[0].key
				} else {
					// Merge
					rightSibling.values.unshift(...node.values)

					// Remove the old pointer to rightSibling
					parent.values.splice(1, 1)

					// Replace the node pointer with the new rightSibling
					const leftMost = parent.values[0].key === null
					parent.values[0] = {
						key: leftMost ? null : rightSibling.values[0].key,
						value: rightSibling.id,
					}
					delete this.nodes[node.id]
				}
			} else {
				const leftSibling = this.nodes[parent.values[parentIndex - 1].value]
				if (!leftSibling) throw new Error("Broken.")

				const combinedSize = leftSibling.values.length + node.values.length
				if (combinedSize > this.maxSize) {
					// Redistribute
					const splitIndex = Math.round(combinedSize / 2)

					const moveRight = leftSibling.values.splice(splitIndex, this.maxSize)
					node.values.unshift(...moveRight)

					// Update parent keys.
					parent.values[parentIndex].key = node.values[0].key
				} else {
					// Merge

					leftSibling.values.push(...node.values)
					// No need to update minKey because we added to the right.
					// Just need to delete the old node.
					parent.values.splice(parentIndex, 1)

					delete this.nodes[node.id]
				}
			}

			// Recur
			node = parent
			continue
		}
	}

	depth() {
		const root = this.nodes["root"]
		if (!root) return 0
		let depth = 1
		let node = root
		while (!node.leaf) {
			depth += 1
			const nextNode = this.nodes[node.values[0].value]
			if (!nextNode) throw new Error("Broken.")
			node = nextNode
		}
		return depth
	}
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}
