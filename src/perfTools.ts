import { Bench } from "tinybench"

function prettyNs(timeNs: number) {
	const round = (n: number) =>
		(Math.round(n * 1000) / 1000).toFixed(3).padStart(7, "0")

	const seconds = timeNs / (1000 * 1000 * 1000)
	if (seconds >= 1) return round(seconds) + "s"

	const ms = timeNs / (1000 * 1000)
	if (ms >= 1) return round(ms) + "ms"

	const us = timeNs / 1000
	if (us >= 1) return round(us) + "μs"

	return round(timeNs) + "ns"
}

export function printTable(bench: Bench) {
	const data = bench.table()
	console.table(
		data.map((item) => {
			if (!item) return
			const { "Average Time (ns)": time, ...rest } = item
			return {
				"Average Time": prettyNs(time as number),
				...rest,
			}
		})
	)
}
