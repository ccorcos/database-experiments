import { RWLock } from "@ccorcos/lock"
import { orderedArray } from "@ccorcos/ordered-array"

/** Used to store tree nodes. */
export type AsyncKeyValueStorage<V = any> = {
	get: (key: string) => Promise<V | undefined>
	write: (tx: {
		set?: { key: string; value: V }[]
		delete?: string[]
	}) => Promise<void>
}

type ReadTransactionApi<T> = {
	get: (key: string) => Promise<T | undefined>
}

export class ReadTransaction<T> implements ReadTransactionApi<T> {
	cache = new Map<string, T | undefined>()

	constructor(public storage: AsyncKeyValueStorage<T>) {}

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

export type BranchNode<K, D> = {
	leaf?: false
	id: string
	data: D
	children: { minKey: K | null; data: D; childId: string }[]
}

export type LeafNode<K, V, D> = {
	leaf: true
	id: string
	data: D
	values: { key: K; value: V }[]
}

function compare(a: any, b: any) {
	if (a === b) return 0
	if (a > b) return 1
	return -1
}

export type NodeCursor<K, V, D> = {
	nodePath: (BranchNode<K, D> | LeafNode<K, V, D>)[]
	indexPath: number[]
}

export type TreeReducer<K, V, D> = {
	leaf: (values: LeafNode<K, V, D>["values"]) => D
	branch: (children: BranchNode<K, D>["children"]) => D
}

export function combineTreeReducers<
	A extends { [key: string]: TreeReducer<any, any, any> }
>(reducers: A) {
	const combined: TreeReducer<
		A[keyof A] extends TreeReducer<infer K, any, any> ? K : never,
		A[keyof A] extends TreeReducer<any, infer V, any> ? V : never,
		{ [K in keyof A]: ReturnType<A[K]["leaf"]> }
	> = {
		leaf: (values) => {
			const data: any = {}
			for (const [key, reducer] of Object.entries(reducers)) {
				data[key] = reducer.leaf(values)
			}
			return data
		},
		branch: (children) => {
			const data: any = {}
			for (const [key, reducer] of Object.entries(reducers)) {
				data[key] = reducer.branch(
					children.map((child) => ({ ...child, data: child.data[key] }))
				)
			}
			return data
		},
	}
	return combined
}

export class AsyncBinaryPlusReducerTree<K = string | number, V = any, D = any> {
	/**
	 * minSize must be less than maxSize / 2.
	 */
	constructor(
		public storage: AsyncKeyValueStorage<BranchNode<K, D> | LeafNode<K, V, D>>,
		public minSize: number,
		public maxSize: number,
		public reducer: TreeReducer<K, V, D>,
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
		tx: ReadTransactionApi<BranchNode<K, D> | LeafNode<K, V, D>>,
		key: K
	): Promise<NodeCursor<K, V, D>> {
		const nodePath: (BranchNode<K, D> | LeafNode<K, V, D>)[] = []
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

			const leaf = nodePath[0] as LeafNode<K, V, D>
			const result = this.leafValues.search(leaf.values, key)
			if (result.found === undefined) return
			return leaf.values[result.found].value
		})
	}

	private async startCursor(
		tx: ReadTransactionApi<BranchNode<K, D> | LeafNode<K, V, D>>
	) {
		const cursor: NodeCursor<K, V, D> = {
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
		tx: ReadTransactionApi<BranchNode<K, D> | LeafNode<K, V, D>>,
		cursor: NodeCursor<K, V, D>
	) {
		// console.log(cursor)
		cursor = {
			nodePath: [...cursor.nodePath],
			indexPath: [...cursor.indexPath],
		}
		for (let i = 0; i < cursor.nodePath.length - 1; i++) {
			// Find the point in the path where we need to go down a sibling branch.
			const parent = cursor.nodePath[i + 1] as BranchNode<K, D>
			const parentIndex = cursor.indexPath[i]
			const nextIndex = parentIndex + 1
			if (nextIndex >= parent.children.length) continue

			// Here's a branch.
			cursor.indexPath[i] = nextIndex

			// Fix the rest of the cursor.
			for (let j = i; j >= 0; j--) {
				const parent = cursor.nodePath[j + 1] as BranchNode<K, D>
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
		tx: ReadTransactionApi<BranchNode<K, D> | LeafNode<K, V, D>>
	) {
		const cursor: NodeCursor<K, V, D> = {
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
		tx: ReadTransactionApi<BranchNode<K, D> | LeafNode<K, V, D>>,
		cursor: NodeCursor<K, V, D>
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
				const parent = cursor.nodePath[j + 1] as BranchNode<K, D>
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

			let startKey: NodeCursor<K, V, D> | undefined
			let endKey: NodeCursor<K, V, D> | undefined
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
				const leaf = endKey.nodePath[0] as LeafNode<K, V, D>
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

				let cursor: NodeCursor<K, V, D> | undefined = endKey
				while ((cursor = await this.prevCursor(tx, cursor))) {
					const leaf = cursor.nodePath[0] as LeafNode<K, V, D>

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
			const leaf = startKey.nodePath[0] as LeafNode<K, V, D>
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

			let cursor: NodeCursor<K, V, D> | undefined = startKey
			while ((cursor = await this.nextCursor(tx, cursor))) {
				const leaf = cursor.nodePath[0] as LeafNode<K, V, D>

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

	reduce = async (
		args: {
			gt?: K
			gte?: K
			lt?: K
			lte?: K
		} = {}
	) => {
		return this.lock.withRead(async () => {
			const tx = new ReadTransaction(this.storage)

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
					throw new Error("Invalid bounds.")
				}
				if (comp === 0 && (startOpen || endOpen)) {
					throw new Error("Invalid open bounds.")
				}
			}

			let startKey: NodeCursor<K, V, D> | undefined
			let endKey: NodeCursor<K, V, D> | undefined
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

			const startLeaf = startKey.nodePath[0] as LeafNode<K, V, D>
			const endLeaf = endKey.nodePath[0] as LeafNode<K, V, D>

			if (startLeaf.id === endLeaf.id) {
				let startIndex = 0
				let endIndex = endLeaf.values.length
				if (start !== undefined) {
					const result = this.leafValues.search(startLeaf.values, start)
					if (result.found !== undefined) {
						startIndex = result.found + (startOpen ? 1 : 0)
					} else {
						startIndex = result.closest
					}
				}
				if (end !== undefined) {
					const result = this.leafValues.search(endLeaf.values, end)
					if (result.found !== undefined) {
						endIndex = result.found + (endOpen ? 0 : 1)
					} else {
						endIndex = result.closest
					}
				}

				const values = startLeaf.values.slice(startIndex, endIndex)
				const data = this.reducer.leaf(values)
				return data
			}

			let startData: D
			if (start !== undefined) {
				const result = this.leafValues.search(startLeaf.values, start)

				let startIndex: number
				if (result.found !== undefined) {
					startIndex = result.found + (startOpen ? 1 : 0)
				} else {
					startIndex = result.closest
				}

				const startValues = startLeaf.values.slice(startIndex)
				startData = this.reducer.leaf(startValues)
			} else {
				startData = startLeaf.data
			}

			let endData: D
			if (end !== undefined) {
				const result = this.leafValues.search(endLeaf.values, end)

				let endIndex: number
				if (result.found !== undefined) {
					endIndex = result.found + (endOpen ? 0 : 1)
				} else {
					endIndex = result.closest
				}

				const endValues = endLeaf.values.slice(0, endIndex)
				endData = this.reducer.leaf(endValues)
			} else {
				endData = endLeaf.data
			}

			for (let i = 1; i < startKey.nodePath.length; i++) {
				const startBranch = startKey.nodePath[i] as BranchNode<K, D>
				const endBranch = endKey.nodePath[i] as BranchNode<K, D>
				const startIndex = startKey.indexPath[i - 1]
				const endIndex = endKey.indexPath[i - 1]

				const startItem = {
					...startBranch.children[startIndex],
					data: startData,
				}
				const endItem = { ...endBranch.children[endIndex], data: endData }

				if (startBranch.id !== endBranch.id) {
					const startRest = startBranch.children.slice(startIndex + 1)
					startData = this.reducer.branch([startItem, ...startRest])
					const endRest = endBranch.children.slice(0, endIndex)
					endData = this.reducer.branch([...endRest, endItem])
					continue
				}

				if (startIndex === endIndex) throw new Error("This shouldn't happen")

				const middleItems = startBranch.children.slice(startIndex + 1, endIndex)
				const resultData = this.reducer.branch([
					startItem,
					...middleItems,
					endItem,
				])

				return resultData
			}

			throw new Error("This shouldn't happen.")
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
		tx: ReadWriteTransactionApi<BranchNode<K, D> | LeafNode<K, V, D>>,
		key: K,
		value: V
	) {
		const { nodePath, indexPath } = await this.findPath(tx, key)

		// Intitalize root node.
		if (nodePath.length === 0) {
			tx.set("root", {
				leaf: true,
				id: "root",
				data: this.reducer.leaf([{ key, value }]),
				values: [{ key, value }],
			})
			return
		}

		// Insert into leaf node.
		const leaf = nodePath[0] as LeafNode<K, V, D>
		const newLeaf = { ...leaf, values: [...leaf.values] }
		this.leafValues.insert(newLeaf.values, { key, value })
		tx.set(newLeaf.id, newLeaf)
		// NOTE: leaf.data will be updates later.
		nodePath[0] = newLeaf

		// Balance the tree by splitting nodes, starting from the leaf.
		let node = nodePath.shift()
		while (node) {
			const size = node.leaf ? node.values.length : node.children.length

			if (size <= this.maxSize) {
				// No splitting, update count.
				if (node.leaf) {
					const newNode: LeafNode<K, V, D> = {
						...node,
						data: this.reducer.leaf(node.values),
					}
					node = newNode
					tx.set(newNode.id, newNode)
				} else {
					const newNode: BranchNode<K, D> = {
						...node,
						data: this.reducer.branch(node.children),
					}
					node = newNode
					tx.set(newNode.id, newNode)
				}

				// We're at the root.
				if (nodePath.length === 0) break

				// Still need to update the parent data.
				const parent = nodePath.shift() as BranchNode<K, D>
				const parentIndex = indexPath.shift()!

				const newParent: BranchNode<K, D> = {
					...parent,
					children: [...parent.children],
				}
				newParent.children[parentIndex] = {
					...newParent.children[parentIndex],
					data: node.data,
				}
				tx.set(newParent.id, newParent)

				// Recur
				node = newParent
				continue
			}

			const splitIndex = Math.round(size / 2)

			if (node.leaf) {
				const rightValues = node.values.slice(splitIndex)
				const rightNode: LeafNode<K, V, D> = {
					id: randomId(),
					leaf: true,
					data: this.reducer.leaf(rightValues),
					values: rightValues,
				}
				tx.set(rightNode.id, rightNode)
				const rightMinKey = rightNode.values[0].key

				if (node.id === "root") {
					const leftValues = node.values.slice(0, splitIndex)
					const leftNode: LeafNode<K, V, D> = {
						id: randomId(),
						leaf: true,
						data: this.reducer.leaf(leftValues),
						values: leftValues,
					}
					tx.set(leftNode.id, leftNode)

					const rootNodeChildren: BranchNode<K, D>["children"] = [
						{ minKey: null, childId: leftNode.id, data: leftNode.data },
						{
							minKey: rightMinKey,
							childId: rightNode.id,
							data: rightNode.data,
						},
					]
					const rootNode: BranchNode<K, D> = {
						id: "root",
						leaf: false,
						data: this.reducer.branch(rootNodeChildren),
						children: rootNodeChildren,
					}
					tx.set("root", rootNode)
					break
				}

				// Insert right node into parent.
				const nodeValues = node.values.slice(0, splitIndex)
				const newNode: LeafNode<K, V, D> = {
					...node,
					data: this.reducer.leaf(nodeValues),
					values: nodeValues,
				}
				tx.set(newNode.id, newNode)

				const parent = nodePath.shift() as BranchNode<K, D>
				const parentIndex = indexPath.shift()
				if (!parent) throw new Error("Broken.")
				if (parentIndex === undefined) throw new Error("Broken.")

				const newParent: BranchNode<K, D> = {
					...parent,
					children: [...parent.children],
				}
				newParent.children.splice(parentIndex + 1, 0, {
					minKey: rightMinKey,
					childId: rightNode.id,
					data: rightNode.data,
				})
				parent.children[parentIndex] = {
					...parent.children[parentIndex],
					data: newNode.data,
				}

				// Recur
				node = newParent
				continue
			}

			const rightChildren = node.children.slice(splitIndex)
			const rightNode: BranchNode<K, D> = {
				id: randomId(),
				data: this.reducer.branch(rightChildren),
				children: rightChildren,
			}
			tx.set(rightNode.id, rightNode)
			const rightMinKey = rightNode.children[0].minKey

			if (node.id === "root") {
				const leftChildren = node.children.slice(0, splitIndex)
				const leftNode: BranchNode<K, D> = {
					id: randomId(),
					data: this.reducer.branch(leftChildren),
					children: leftChildren,
				}
				tx.set(leftNode.id, leftNode)

				const rootNodeChildren: BranchNode<K, D>["children"] = [
					{ minKey: null, childId: leftNode.id, data: leftNode.data },
					{ minKey: rightMinKey, childId: rightNode.id, data: rightNode.data },
				]
				const rootNode: BranchNode<K, D> = {
					id: "root",
					data: this.reducer.branch(rootNodeChildren),
					children: rootNodeChildren,
				}
				tx.set("root", rootNode)
				break
			}

			// Insert right node into parent.
			const newNodeChildren = node.children.slice(0, splitIndex)
			const newNode: BranchNode<K, D> = {
				...node,
				data: this.reducer.branch(newNodeChildren),
				children: newNodeChildren,
			}
			tx.set(newNode.id, newNode)

			const parent = nodePath.shift() as BranchNode<K, D>
			const parentIndex = indexPath.shift()
			if (!parent) throw new Error("Broken.")
			if (parentIndex === undefined) throw new Error("Broken.")

			const newParent: BranchNode<K, D> = {
				...parent,
				children: [...parent.children],
			}
			newParent.children.splice(parentIndex + 1, 0, {
				minKey: rightMinKey,
				childId: rightNode.id,
				data: rightNode.data,
			})
			newParent.children[parentIndex] = {
				...newParent.children[parentIndex],
				data: newNode.data,
			}
			tx.set(newParent.id, newParent)

			// Recur
			node = newParent
			continue
		}
	}

	private async _delete(
		tx: ReadWriteTransactionApi<BranchNode<K, D> | LeafNode<K, V, D>>,
		key: K
	) {
		const root = await tx.get("root")
		if (!root) return

		// Delete from leaf node.
		const nodePath = [root]
		const indexPath: number[] = []
		while (true) {
			const node = nodePath[0]

			if (node.leaf) {
				const newNode: LeafNode<K, V, D> = { ...node, values: [...node.values] }
				const exists = this.leafValues.remove(newNode.values, key)
				if (!exists) return // No changes to the tree!
				tx.set(newNode.id, newNode)

				// Continue to rebalance
				nodePath[0] = newNode
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
			if (node.leaf) {
				node = { ...node }
				node.data = this.reducer.leaf(node.values)
			} else {
				node = { ...node }
				node.data = this.reducer.branch(node.children)
			}
			tx.set(node.id, node)

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

			let parent = nodePath.shift() as BranchNode<K, D>
			const parentIndex = indexPath.shift()
			if (!parent) throw new Error("Broken.")
			if (parentIndex === undefined) throw new Error("Broken.")

			const size = node.leaf ? node.values.length : node.children.length
			const minKey = node.leaf ? node.values[0].key : node.children[0].minKey

			let dataUpdated = false
			const parentCount = parent.children[parentIndex].data
			if (parentCount !== node.data) {
				dataUpdated = true
				parent = { ...parent, children: [...parent.children] }
				parent.children[parentIndex] = {
					...parent.children[parentIndex],
					data: node.data,
				}
				tx.set(parent.id, parent)
			}

			// No need to merge but we might need to update the minKey in the parent
			let minKeyUpdated = false
			if (size >= this.minSize) {
				const parentItem = parent.children[parentIndex]
				// No need to recusively update the left-most branch.
				// No need to recursively update if the minKey didn't change.
				if (
					parentItem.minKey !== null &&
					this.compareBranchKey(parentItem.minKey, minKey) !== 0
				) {
					// Set the minKey
					minKeyUpdated = true
					parent = { ...parent, children: [...parent.children] }
					parent.children[parentIndex] = {
						...parent.children[parentIndex],
						minKey,
					}
					tx.set(parent.id, parent)
				}

				if (!minKeyUpdated && !dataUpdated) {
					return
				}

				// Recur
				node = parent
				continue
			}

			// Merge or redistribute leaf nodes.
			if (node.leaf) {
				if (parentIndex === 0) {
					const rightId = parent.children[parentIndex + 1].childId
					const rightSibling = (await tx.get(rightId)) as typeof node
					if (!rightSibling) throw new Error("Broken.")

					const combinedSize = node.values.length + rightSibling.values.length

					// Redistribute leaf.
					if (combinedSize > this.maxSize) {
						const splitIndex = Math.round(combinedSize / 2) - node.values.length

						const newRight: LeafNode<K, V, D> = {
							...rightSibling,
							values: [...rightSibling.values],
						}
						const moveLeft = newRight.values.splice(0, splitIndex)
						newRight.data = this.reducer.leaf(newRight.values)
						tx.set(newRight.id, newRight)

						const newNode: LeafNode<K, V, D> = {
							...node,
							values: [...node.values],
						}
						newNode.values.push(...moveLeft)
						newNode.data = this.reducer.leaf(newNode.values)
						tx.set(newNode.id, newNode)

						// Update parent minKey.
						const newParent: BranchNode<K, D> = {
							...parent,
							children: [...parent.children],
						}
						if (newParent.children[parentIndex].minKey !== null) {
							const leftMinKey = newNode.values[0].key
							newParent.children[parentIndex] = {
								minKey: leftMinKey,
								childId: newNode.id,
								data: newNode.data,
							}
						} else {
							newParent.children[parentIndex] = {
								...newParent.children[parentIndex],
								data: newNode.data,
							}
						}
						const rightMinKey = newRight.values[0].key
						newParent.children[parentIndex + 1] = {
							minKey: rightMinKey,
							childId: newRight.id,
							data: newRight.data,
						}
						tx.set(newParent.id, newParent)

						// Recur
						node = newParent
						continue
					}

					// Merge leaves.
					const newRight: LeafNode<K, V, D> = {
						...rightSibling,
						values: [...rightSibling.values],
					}
					newRight.values.unshift(...node.values)
					newRight.data = this.reducer.leaf(newRight.values)

					// Remove the old pointer to rightSibling
					const newParent: BranchNode<K, D> = {
						...parent,
						children: [...parent.children],
					}
					newParent.children.splice(1, 1)

					// Replace the node pointer with the new rightSibling
					const leftMost = newParent.children[0].minKey === null
					newParent.children[0] = {
						minKey: leftMost ? null : newRight.values[0].key,
						childId: newRight.id,
						data: newRight.data,
					}
					tx.set(newRight.id, newRight)
					tx.set(newParent.id, newParent)
					tx.delete(node.id)

					// Recur
					node = newParent
					continue
				}

				// Merge or redistribute with left sibling.
				const leftId = parent.children[parentIndex - 1].childId
				const leftSibling = (await tx.get(leftId)) as typeof node
				if (!leftSibling) throw new Error("Broken.")

				const combinedSize = leftSibling.values.length + node.values.length

				// Redistribute leaf.
				if (combinedSize > this.maxSize) {
					const splitIndex = Math.round(combinedSize / 2)

					const newLeft: LeafNode<K, V, D> = {
						...leftSibling,
						values: [...leftSibling.values],
					}

					const moveRight = newLeft.values.splice(splitIndex, this.maxSize)

					const newNode: LeafNode<K, V, D> = {
						...node,
						values: [...node.values],
					}
					newNode.values.unshift(...moveRight)

					newLeft.data = this.reducer.leaf(newLeft.values)
					newNode.data = this.reducer.leaf(newNode.values)

					// Update parent keys.
					const newParent: BranchNode<K, D> = {
						...parent,
						children: [...parent.children],
					}

					newParent.children[parentIndex] = {
						minKey: newNode.values[0].key,
						childId: newNode.id,
						data: newNode.data,
					}
					parent.children[parentIndex - 1] = {
						...parent.children[parentIndex - 1],
						data: newLeft.data,
					}

					tx.set(newLeft.id, newLeft)
					tx.set(newNode.id, newNode)
					tx.set(newParent.id, newParent)

					// Recur
					node = newParent
					continue
				}

				// Merge
				const newLeft: LeafNode<K, V, D> = {
					...leftSibling,
					values: [...leftSibling.values],
				}
				newLeft.values.push(...node.values)
				newLeft.data = this.reducer.leaf(newLeft.values)

				// Just need to delete the old node.
				const newParent: BranchNode<K, D> = {
					...parent,
					children: [...parent.children],
				}
				newParent.children.splice(parentIndex, 1)

				// No need to update minKey because we added to the right.
				newParent.children[parentIndex - 1] = {
					...newParent.children[parentIndex - 1],
					data: leftSibling.data,
				}

				tx.set(newLeft.id, newLeft)
				tx.set(newParent.id, newParent)
				tx.delete(node.id)

				// Recur
				node = newParent
				continue
			}

			// Merge or redistribute branch nodes.
			if (parentIndex === 0) {
				const rightId = parent.children[parentIndex + 1].childId
				const rightSibling = (await tx.get(rightId)) as typeof node
				if (!rightSibling) throw new Error("Broken.")

				const combinedSize = node.children.length + rightSibling.children.length

				// Redistribute leaf.
				if (combinedSize > this.maxSize) {
					const splitIndex = Math.round(combinedSize / 2) - node.children.length

					const newRight: BranchNode<K, D> = {
						...rightSibling,
						children: [...rightSibling.children],
					}
					const moveLeft = newRight.children.splice(0, splitIndex)
					newRight.data = this.reducer.branch(newRight.children)
					tx.set(newRight.id, newRight)

					const newNode: BranchNode<K, D> = {
						...node,
						children: [...node.children],
					}
					newNode.children.push(...moveLeft)
					newNode.data = this.reducer.branch(newNode.children)
					tx.set(newNode.id, newNode)

					// Update parent minKey.
					const newParent: BranchNode<K, D> = {
						...parent,
						children: [...parent.children],
					}
					if (newParent.children[parentIndex].minKey !== null) {
						const leftMinKey = newNode.children[0].minKey
						newParent.children[parentIndex] = {
							minKey: leftMinKey,
							childId: newNode.id,
							data: newNode.data,
						}
					} else {
						newParent.children[parentIndex] = {
							...newParent.children[parentIndex],
							data: newNode.data,
						}
					}
					const rightMinKey = newRight.children[0].minKey
					newParent.children[parentIndex + 1] = {
						minKey: rightMinKey,
						childId: newRight.id,
						data: newRight.data,
					}
					tx.set(newParent.id, newParent)

					// Recur
					node = newParent
					continue
				}

				// Merge leaves.
				const newRight: BranchNode<K, D> = {
					...rightSibling,
					children: [...rightSibling.children],
				}
				newRight.children.unshift(...node.children)
				newRight.data = this.reducer.branch(newRight.children)

				// Remove the old pointer to rightSibling
				const newParent: BranchNode<K, D> = {
					...parent,
					children: [...parent.children],
				}
				newParent.children.splice(1, 1)

				// Replace the node pointer with the new rightSibling
				const leftMost = newParent.children[0].minKey === null
				newParent.children[0] = {
					minKey: leftMost ? null : newRight.children[0].minKey,
					childId: newRight.id,
					data: newRight.data,
				}
				tx.set(newRight.id, newRight)
				tx.set(newParent.id, newParent)
				tx.delete(node.id)

				// Recur
				node = newParent
				continue
			}

			// Merge or redistribute with left sibling.
			const leftId = parent.children[parentIndex - 1].childId
			const leftSibling = (await tx.get(leftId)) as typeof node
			if (!leftSibling) throw new Error("Broken.")

			const combinedSize = leftSibling.children.length + node.children.length

			// Redistribute branch.
			if (combinedSize > this.maxSize) {
				const splitIndex = Math.round(combinedSize / 2)

				const newLeft: BranchNode<K, D> = {
					...leftSibling,
					children: [...leftSibling.children],
				}
				const moveRight = newLeft.children.splice(splitIndex, this.maxSize)
				newLeft.data = this.reducer.branch(newLeft.children)

				const newNode: BranchNode<K, D> = {
					...node,
					children: [...node.children],
				}
				newNode.children.unshift(...moveRight)
				newNode.data = this.reducer.branch(newNode.children)

				// Update parent keys.
				const newParent: BranchNode<K, D> = {
					...parent,
					children: [...parent.children],
				}
				newParent.children[parentIndex] = {
					minKey: newNode.children[0].minKey,
					childId: newNode.id,
					data: newNode.data,
				}
				newParent.children[parentIndex - 1] = {
					...newParent.children[parentIndex - 1],
					data: newLeft.data,
				}

				tx.set(newLeft.id, newLeft)
				tx.set(newNode.id, newNode)
				tx.set(newParent.id, newParent)

				// Recur
				node = newParent
				continue
			}

			// Merge
			const newLeft: BranchNode<K, D> = {
				...leftSibling,
				children: [...leftSibling.children],
			}
			newLeft.children.push(...node.children)
			newLeft.data = this.reducer.branch(newLeft.children)

			// Just need to delete the old node.
			const newParent: BranchNode<K, D> = {
				...parent,
				children: [...parent.children],
			}
			newParent.children.splice(parentIndex, 1)

			// No need to update minKey because we added to the right.
			parent.children[parentIndex - 1] = {
				...parent.children[parentIndex - 1],
				data: leftSibling.data,
			}

			tx.set(newLeft.id, newLeft)
			tx.set(newParent.id, newParent)
			tx.delete(node.id)

			// Recur
			node = newParent
			continue
		}
	}
}

function randomId() {
	return Math.random().toString(36).slice(2, 10)
}
