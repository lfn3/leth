package net.lfn3.leth

open class Computed<T, U>(log: LogReader<T>, init: U, op: (acc : U, T) -> U) {
    protected var value : U = init

    init {
        value = log.fold(init, op)
        val boundOp : (T) -> Unit = {
            val result = op(value, it)
            value = result
        }
        log.tail(boundOp)
    }
}
