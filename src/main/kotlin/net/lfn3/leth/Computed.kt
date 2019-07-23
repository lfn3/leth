package net.lfn3.leth

open class Computed<T, U>(log: LogReader<T>, op: (T, U?) -> U?) {
    init {
        val boundOp : (T) -> Unit = {
            val result = op(it, value)
            value = result
        }
        log.forEach(boundOp)
        log.tail(boundOp)
    }

    protected var value : U? = null
}
