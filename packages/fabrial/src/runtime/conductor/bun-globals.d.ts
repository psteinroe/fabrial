// SHIM(conductor#9): pgconductor-js source references Bun's global `Timer` type.
type Timer = ReturnType<typeof setTimeout>;
