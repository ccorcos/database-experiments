import {
	BranchNode,
	InMemoryReducerTree,
	LeafNode,
	TreeReducer,
} from "./InMemoryReducerTree"

function intervalReducer<I extends [any, any]>(
	reduce: (acc: I, interval: I) => I
): TreeReducer<[...I, any], any, I> {
	return {
		leaf: (values) => {
			let bound = values[0].key.slice(0, 2) as I
			for (let i = 1; i < values.length; i++) {
				bound = reduce(bound, values[i].key.slice(0, 2) as I)
			}
			return bound
		},
		branch: (children) => {
			let bound = children[0].data
			for (let i = 1; i < children.length; i++) {
				bound = reduce(bound, children[i].data)
			}
			return bound
		},
	}
}

export class BinaryPlusIntervalTree<
	B,
	K extends [B, B, any],
	V = any
> extends InMemoryReducerTree<K, V, [B, B]> {
	constructor(
		public minSize: number,
		public maxSize: number,
		public reduceInterval: (acc: [B, B], interval: [B, B]) => [B, B],
		public compareKey: (a: K, b: K) => number,
		public compareBound: (a: B, b: B) => number
	) {
		const reducer = intervalReducer(reduceInterval)
		super(minSize, maxSize, reducer, compareKey)
	}

	private boundsOverlap(a: [B, B], b: [B, B]) {
		const [min, max] = a
		const [start, end] = b

		// return max >= start && min <= end
		return (
			this.compareBound(max, start) >= 0 && this.compareBound(min, end) <= 0
		)
	}

	overlaps([start, end]: [B, B]) {
		const root = this.nodes["root"]
		if (!root) return []

		if (root.leaf) {
			return root.values.filter((item) => {
				const [min, max] = item.key
				return this.boundsOverlap([start, end], [min, max])
			})
		}

		{
			// No results.
			const [min, max] = root.data
			if (!this.boundsOverlap([start, end], [min, max])) {
				return []
			}
		}

		// I'm not sure if we'd ever want to `limit` on this kind of query. But if we did, then we'd want
		// to do a depth-first traversal more lazily.
		let layer = [root]
		while (true) {
			const nextLayerIds: string[] = []
			for (const node of layer) {
				for (const child of node.children) {
					const [min, max] = child.data
					if (this.boundsOverlap([start, end], [min, max])) {
						nextLayerIds.push(child.childId)
					}
				}
			}

			if (nextLayerIds.length === 0) return []

			const nextLayer = nextLayerIds.map((childId) => {
				const node = this.nodes[childId]
				if (!node) throw new Error("Broken.")
				return node
			})

			// Recur until we get to the leaves.
			if (!nextLayer[0].leaf) {
				layer = nextLayer as BranchNode<K, [B, B]>[]
				continue
			}

			const leaves = nextLayer as LeafNode<K, V, [B, B]>[]
			const result: { key: K; value: V }[] = []
			for (const leaf of leaves) {
				for (const item of leaf.values) {
					const [min, max] = item.key
					if (this.boundsOverlap([start, end], [min, max])) {
						result.push(item)
					}
				}
			}
			return result
		}
	}
}
