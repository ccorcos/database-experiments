/*

- better types.
- tuple keys
- list query
---
- count aggregation
- generalize to GiST
- interval range query

*/

import { orderedArray } from "@ccorcos/ordered-array"

type Key = string | number

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

export class BinaryPlusTree2<K = string | number, V = any> {
	// In preparation for storing nodes in a key-value database.
	nodes: { [key: Key]: BranchNode<K> | LeafNode<K, V> | undefined } = {}

	/**
	 * minSize must be less than maxSize / 2.
	 */
	constructor(
		public minSize: number,
		public maxSize: number,
		public compareKey: (a: K, b: K) => number = compare
	) {
		if (minSize > maxSize / 2) throw new Error("Invalid tree size.")
	}

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

	get = (key: K): V | undefined => {
		const root = this.nodes["root"]
		if (!root) return // Empty tree

		let node = root
		while (true) {
			if (node.leaf) {
				const result = this.leafValues.search(node.values, key)
				if (result.found === undefined) return
				return node.values[result.found].value
			}

			const result = this.branchChildren.search(node.children, key)

			// Closest key that is at least as big as the key...
			// So the closest should never be less than the minKey.
			if (result.closest === 0) throw new Error("Broken.")

			const childIndex =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.children[childIndex].childId
			const child = this.nodes[childId]
			if (!child) throw Error("Missing child node.")
			node = child
		}
	}

	set = (key: K, value: V) => {
		const root = this.nodes["root"]

		// Intitalize root node.
		if (!root) {
			this.nodes["root"] = {
				leaf: true,
				id: "root",
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
				const existing = this.leafValues.insert(node.values, { key, value })
				// No need to rebalance if we're replacing
				if (existing) return
				break
			}

			const result = this.branchChildren.search(node.children, key)
			const index =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.children[index].childId
			const child = this.nodes[childId]
			if (!child) throw Error("Missing child node.")
			// Recur into child.
			nodePath.unshift(child)
			indexPath.unshift(index)
		}

		// Balance the tree by splitting nodes, starting from the leaf.
		let node = nodePath.shift()
		while (node) {
			const size = node.leaf ? node.values.length : node.children.length
			if (size <= this.maxSize) break
			const splitIndex = Math.round(size / 2)

			if (node.leaf) {
				// NOTE: this mutates the array!
				const rightValues = node.values.splice(splitIndex)
				const rightNode: LeafNode<K, V> = {
					id: randomId(),
					leaf: true,
					values: rightValues,
				}
				this.nodes[rightNode.id] = rightNode
				const rightMinKey = rightNode.values[0].key

				if (node.id === "root") {
					const leftNode: LeafNode<K, V> = {
						id: randomId(),
						leaf: true,
						// NOTE: this array was mutated above.
						values: node.values,
					}
					this.nodes[leftNode.id] = leftNode
					const rootNode: BranchNode<K> = {
						id: "root",
						leaf: false,
						children: [
							{ minKey: null, childId: leftNode.id },
							{ minKey: rightMinKey, childId: rightNode.id },
						],
					}
					this.nodes["root"] = rootNode
					break
				}

				// Insert right node into parent.
				const parent = nodePath.shift() as BranchNode<K>
				const parentIndex = indexPath.shift()
				if (!parent) throw new Error("Broken.")
				if (parentIndex === undefined) throw new Error("Broken.")
				parent.children.splice(parentIndex + 1, 0, {
					minKey: rightMinKey,
					childId: rightNode.id,
				})

				// Recur
				node = parent
				continue
			}

			// NOTE: this mutates the array!
			const rightChildren = node.children.splice(splitIndex)
			const rightNode: BranchNode<K> = {
				id: randomId(),
				children: rightChildren,
			}
			this.nodes[rightNode.id] = rightNode
			const rightMinKey = rightNode.children[0].minKey

			if (node.id === "root") {
				const leftNode: BranchNode<K> = {
					id: randomId(),
					// NOTE: this array was mutated above.
					children: node.children,
				}
				this.nodes[leftNode.id] = leftNode
				const rootNode: BranchNode<K> = {
					id: "root",
					children: [
						{ minKey: null, childId: leftNode.id },
						{ minKey: rightMinKey, childId: rightNode.id },
					],
				}
				this.nodes["root"] = rootNode
				break
			}

			// Insert right node into parent.
			const parent = nodePath.shift() as BranchNode<K>
			const parentIndex = indexPath.shift()
			if (!parent) throw new Error("Broken.")
			if (parentIndex === undefined) throw new Error("Broken.")
			parent.children.splice(parentIndex + 1, 0, {
				minKey: rightMinKey,
				childId: rightNode.id,
			})

			// Recur
			node = parent
		}
	}

	delete = (key: K) => {
		const root = this.nodes["root"]
		if (!root) return

		// Delete from leaf node.
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			const node = nodePath[0]

			if (node.leaf) {
				const exists = this.leafValues.remove(node.values, key)
				if (!exists) return // No changes to the tree!
				break
			}

			// Recur into the child.
			const result = this.branchChildren.search(node.children, key)
			const index =
				result.found !== undefined ? result.found : result.closest - 1
			const childId = node.children[index].childId
			const child = this.nodes[childId]
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
					delete this.nodes["root"]
					return
				}

				// A root node with one child becomes its child.
				if (node.children.length === 1) {
					const childId = node.children[0].childId
					const childNode = this.nodes[childId]
					if (!childNode) throw new Error("Broken.")
					this.nodes["root"] = { ...childNode, id: "root" }
					delete this.nodes[childId]
				}

				return
			}

			const parent = nodePath.shift() as BranchNode<K>
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
				parentItem.minKey = minKey
				node = parent
				continue
			}

			// Merge or redistribute leaf nodes.
			if (node.leaf) {
				if (parentIndex === 0) {
					const rightId = parent.children[parentIndex + 1].childId
					const rightSibling = this.nodes[rightId] as typeof node
					if (!rightSibling) throw new Error("Broken.")

					const combinedSize = node.values.length + rightSibling.values.length

					// Redistribute leaf.
					if (combinedSize > this.maxSize) {
						const splitIndex = Math.round(combinedSize / 2) - node.values.length
						// NOTE: this mutates the array!
						const moveLeft = rightSibling.values.splice(0, splitIndex)
						node.values.push(...moveLeft)
						// Update parent minKey.
						if (parent.children[parentIndex].minKey !== null) {
							const leftMinKey = node.values[0].key
							parent.children[parentIndex].minKey = leftMinKey
						}
						const rightMinKey = rightSibling.values[0].key
						parent.children[parentIndex + 1].minKey = rightMinKey

						// Recur
						node = parent
						continue
					}

					// Merge leaves.
					node.values.push(...rightSibling.values)
					// Delete rightSibling
					parent.children.splice(1, 1)
					delete this.nodes[rightSibling.id]
					// Update parent minKey
					const leftMost = parent.children[0].minKey === null
					const minKey = leftMost ? null : node.values[0].key
					parent.children[0].minKey = minKey

					// Recur
					node = parent
					continue
				}

				const leftId = parent.children[parentIndex - 1].childId
				const leftSibling = this.nodes[leftId] as typeof node
				if (!leftSibling) throw new Error("Broken.")

				const combinedSize = leftSibling.values.length + node.values.length

				// Redistribute leaf.
				if (combinedSize > this.maxSize) {
					const splitIndex = Math.round(combinedSize / 2)

					const moveRight = leftSibling.values.splice(splitIndex, this.maxSize)
					node.values.unshift(...moveRight)

					// Update parent minKey.
					parent.children[parentIndex].minKey = node.values[0].key

					// Recur
					node = parent
					continue
				}

				// Merge leaf.
				leftSibling.values.push(...node.values)
				// Delete the node
				parent.children.splice(parentIndex, 1)
				delete this.nodes[node.id]
				// No need to update minKey because we added to the right.

				// Recur
				node = parent
				continue
			}

			// Merge or redistribute branch nodes.
			if (parentIndex === 0) {
				const rightId = parent.children[parentIndex + 1].childId
				const rightSibling = this.nodes[rightId] as typeof node
				if (!rightSibling) throw new Error("Broken.")

				const combinedSize = node.children.length + rightSibling.children.length

				// Redistribute leaf.
				if (combinedSize > this.maxSize) {
					const splitIndex = Math.round(combinedSize / 2) - node.children.length
					// NOTE: this mutates the array!
					const moveLeft = rightSibling.children.splice(0, splitIndex)
					node.children.push(...moveLeft)
					// Update parent minKey.
					if (parent.children[parentIndex].minKey !== null) {
						const leftMinKey = node.children[0].minKey
						parent.children[parentIndex].minKey = leftMinKey
					}
					const rightMinKey = rightSibling.children[0].minKey
					parent.children[parentIndex + 1].minKey = rightMinKey

					// Recur
					node = parent
					continue
				}

				// Merge leaves.
				node.children.push(...rightSibling.children)
				// Delete rightSibling
				parent.children.splice(1, 1)
				delete this.nodes[rightSibling.id]
				// Update parent minKey
				const leftMost = parent.children[0].minKey === null
				const minKey = leftMost ? null : node.children[0].minKey
				parent.children[0].minKey = minKey

				// Recur
				node = parent
				continue
			}

			const leftId = parent.children[parentIndex - 1].childId
			const leftSibling = this.nodes[leftId] as typeof node
			if (!leftSibling) throw new Error("Broken.")

			const combinedSize = leftSibling.children.length + node.children.length

			// Redistribute leaf.
			if (combinedSize > this.maxSize) {
				const splitIndex = Math.round(combinedSize / 2)

				const moveRight = leftSibling.children.splice(splitIndex, this.maxSize)
				node.children.unshift(...moveRight)

				// Update parent minKey.
				parent.children[parentIndex].minKey = node.children[0].minKey

				// Recur
				node = parent
				continue
			}

			// Merge leaf.
			leftSibling.children.push(...node.children)
			// Delete the node
			parent.children.splice(parentIndex, 1)
			delete this.nodes[node.id]
			// No need to update minKey because we added to the right.

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
			const nextNode = this.nodes[node.children[0].childId]
			if (!nextNode) throw new Error("Broken.")
			node = nextNode
		}
		return depth
	}
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}
