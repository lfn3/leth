package net.lfn3.leth

abstract class HeadComputed <T, U>(log: LogReader<T>, init: U, op: (acc : U, T) -> U) {
    protected var value : U = init

    init {
        val head = log.head()
        if (head != null) {
            value = op(init, head)
        }

        val boundOp : (T) -> Unit = {
            val result = op(value, it)
            value = result
        }
        log.tail(boundOp)
    }
}