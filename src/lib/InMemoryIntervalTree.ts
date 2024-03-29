import {
	BranchNode,
	InMemoryReducerTree,
	LeafNode,
	TreeReducer,
} from "./InMemoryReducerTree"

export class InMemoryIntervalTree<
	K extends [B, B, ...any[]],
	V = any,
	B = any,
> extends InMemoryReducerTree<K, V, [B, B]> {
	constructor(
		public minSize: number,
		public maxSize: number,
		public compareKey: (a: K, b: K) => number,
		public compareBound: (a: B, b: B) => number
	) {
		const reducer: TreeReducer<K, V, [B, B]> = {
			leaf: (values) => {
				let a = values[0].key.slice(0, 2) as [B, B]
				for (let i = 1; i < values.length; i++) {
					const b = values[i].key.slice(0, 2) as [B, B]
					a = [
						this.compareBound(a[0], b[0]) <= 0 ? a[0] : b[0],
						this.compareBound(a[1], b[1]) >= 0 ? a[1] : b[1],
					]
				}
				return a
			},
			branch: (children) => {
				let a = children[0].data
				for (let i = 1; i < children.length; i++) {
					const b = children[i].data
					a = [
						this.compareBound(a[0], b[0]) <= 0 ? a[0] : b[0],
						this.compareBound(a[1], b[1]) >= 0 ? a[1] : b[1],
					]
				}
				return a
			},
		}

		super(minSize, maxSize, reducer, compareKey)
	}

	private boundsOverlap(args: { gt?: B; gte?: B; lt?: B; lte?: B }, b: [B, B]) {
		const [start, end] = b

		if (args.gt !== undefined) {
			if (this.compareBound(end, args.gt) <= 0) return false
		} else if (args.gte !== undefined) {
			if (this.compareBound(end, args.gte) < 0) return false
		}

		if (args.lt !== undefined) {
			if (this.compareBound(start, args.lt) >= 0) return false
		} else if (args.lte !== undefined) {
			if (this.compareBound(start, args.lte) > 0) return false
		}

		return true
	}

	overlaps(args: { gt?: B; gte?: B; lt?: B; lte?: B } = {}) {
		const root = this.nodes.get("root")

		if (!root) return []

		if (root.leaf) {
			return root.values.filter((item) => {
				const [min, max] = item.key
				return this.boundsOverlap(args, [min, max])
			})
		}

		{
			// No results.
			const [min, max] = root.data
			if (!this.boundsOverlap(args, [min, max])) {
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
					if (this.boundsOverlap(args, [min, max])) {
						nextLayerIds.push(child.childId)
					}
				}
			}

			if (nextLayerIds.length === 0) {
				return []
			}

			const nextLayer = nextLayerIds.map((childId) => {
				const node = this.nodes.get(childId)
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
					if (this.boundsOverlap(args, [min, max])) {
						result.push(item)
					}
				}
			}
			return result
		}
	}
}
