// TODO: waiting on PR https://github.com/rocicorp/lock/pull/10
// In the meantime, using `npm link /Users/chet/Code/external/lock`

import { RWLockMap } from "@rocicorp/lock"

type LockCmd = { [key: string]: "r" | "rw" | undefined }

export class ConcurrencyLocks extends RWLockMap {
	private async multiLock(cmd: LockCmd) {
		const releases = await Promise.all(
			Object.entries(cmd).map(([key, value]) => {
				if (value === "r") return this.read(key)
				if (value === "rw") return this.write(key)
			})
		)

		let called = false
		return () => {
			if (called) return
			called = true
			for (const release of releases) if (release) release()
		}
	}

	async run<T>(fn: () => AsyncGenerator<LockCmd, T, () => void>) {
		const gen = fn()
		let nextValue = await gen.next()
		const releases = new Set<() => void>()
		while (!nextValue.done) {
			const release = await this.multiLock(nextValue.value)
			releases.add(release)
			nextValue = await gen.next(release)
		}
		for (const release of releases) release()
		const result = nextValue.value
		return result
	}
}
