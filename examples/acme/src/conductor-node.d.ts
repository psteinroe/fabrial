// The pinned Conductor source uses Bun's Timer name; Node's timer has the same role.
// Its package-local ambient shim is not included transitively by TypeScript.
type Timer = ReturnType<typeof setTimeout>;
