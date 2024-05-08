import { PositionSource } from "position-strings"

const positions: string[] = []

function insert(p: PositionSource, index: number, len: number) {
	for (let i = index; i < index + len; i++) {
		// console.log(positions, positions[i - 1])
		positions.splice(i, 0, p.createBetween(positions[i - 1], positions[i]))
	}
}

const p = new PositionSource({ ID: "A" })
insert(p, 0, 5)
console.log(positions)

// [
// 	"A.B", <- A is the waypoint, B is the lex position.
// 	"A.D",
// 	"A.F",
// 	"A.H",
// 	"A.J"
// ]

insert(p, 2, 5)
console.log(positions)

// [
// 	"A.B",
// 	"A.D",
// 	"A.D0B", <- 0 implies insert on the "left side", and B is the nested lex position.
// 	"A.D0D", <-
// 	"A.D0F", <-
// 	"A.D0H", <-
// 	"A.D0J", <-
// 	"A.F",
// 	"A.H",
// 	"A.J"
// ]

const p2 = new PositionSource({ ID: "B" })
insert(p2, 3, 2)
console.log(positions)

// [
// 	"A.B",
// 	"A.D",
// 	"A.D0B",
// 	"A.D0B,B.B", <- `A.D0B,B` is the waypoint, B is the nested lex position.
// 	"A.D0B,B.D", <-
// 	"A.D0D",
// 	"A.D0F",
// 	"A.D0H",
// 	"A.D0J",
// 	"A.F",
// 	"A.H",
// 	"A.J"
// ]

insert(p, 3, 2)
console.log(positions)

// [
// 	"A.B",
// 	"A.D",
// 	"A.D0B",
// 	"A.D0B,B.A1B", <- what's going on here?
// 	"A.D0B,B.A1D", <-
// 	"A.D0B,B.B",
// 	"A.D0B,B.D",
// 	"A.D0D",
// 	"A.D0F",
// 	"A.D0H",
// 	"A.D0J",
// 	"A.F",
// 	"A.H",
// 	"A.J"
// ]

insert(p2, 6, 2)
console.log(positions)

// [
// 	"A.B",
// 	"A.D",
// 	"A.D0B",
// 	"A.D0B,B.A1B",
// 	"A.D0B,B.A1D",
// 	"A.D0B,B.B",
// 	"A.D0B,B.B0B", <-
// 	"A.D0B,B.B0D", <-
// 	"A.D0B,B.D",
// 	"A.D0D",
// 	"A.D0F",
// 	"A.D0H",
// 	"A.D0J",
// 	"A.F",
// 	"A.H",
// 	"A.J"
// ]

insert(p2, 3, 2)
insert(p, 3, 2)
insert(p2, 3, 2)
console.log(positions)

// [
// 	"A.B",
// 	"A.D",
// 	"A.D0B",
// 	"A.D0B,B.A1A0A1A0B",
// 	"A.D0B,B.A1A0A1A0D",
// 	"A.D0B,B.A1A0A1B",
// 	"A.D0B,B.A1A0A1D",
// 	"A.D0B,B.A1A0B",
// 	"A.D0B,B.A1A0D",
// 	"A.D0B,B.A1B",
// 	"A.D0B,B.A1D",
// 	"A.D0B,B.B",
// 	"A.D0B,B.B0B",
// 	"A.D0B,B.B0D",
// 	"A.D0B,B.D",
// 	"A.D0D",
// 	"A.D0F",
// 	"A.D0H",
// 	"A.D0J",
// 	"A.F",
// 	"A.H",
// 	"A.J"
// ]

// What if we used structure instead of strings.

// [Id, Pos]
// [Id, Pos, Left/Right, Pos]
// [Id, Pos, Left/Right, Pos]

type LR = 0 | 1

type Index =
	| [{ id: string; pos: string }]
	| [{ id: string; pos: string }, ...Array<{ lr: LR; pos: string }>]
	| [
			{ id: string; pos: string },
			...Array<{ lr: LR; pos: string }>,
			{ id: string; pos: string },
	  ]
	| [
			{ id: string; pos: string },
			...Array<{ lr: LR; pos: string }>,
			{ id: string; pos: string },
			...Array<{ lr: LR; pos: string }>,
	  ]

positions.splice(0, 9999)

insert(p, 0, 20)

insert(p2, 20, 2)
console.log(positions)
