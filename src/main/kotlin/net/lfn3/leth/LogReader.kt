package net.lfn3.leth

interface LogReader<T> : Iterable<T> {
    fun head() : T?
    fun headWithSeq() : Pair<Long, T>?
    fun get(sequence: Long) : T?
    fun tail(fn : (T) -> Unit) {
        tail(0, fn)
    }
    fun tail(from : Long, fn : (T) -> Unit)
    val size: Long
    fun isEmpty(): Boolean = size == 0L

    companion object {
        fun <T, U> map(log: LogReader<T>, f : (T) -> U) : LogReader<U> {
            return object : LogReader<U> {
                override fun head(): U? {
                    val head = log.head() ?: return null
                    return f(head)
                }

                override fun headWithSeq(): Pair<Long, U>? {
                    val (seq, head) = log.headWithSeq() ?: return null
                    return Pair(seq, f(head))
                }

                override fun get(sequence: Long): U? {
                    val v = log.get(sequence) ?: return null
                    return f(v)
                }

                override fun tail(fn: (newEntry: U) -> Unit) {
                    log.tail { fn(f(it)) }
                }

                override fun tail(from: Long, fn: (U) -> Unit) {
                    log.tail(from) { fn(f(it)) }
                }

                override val size: Long
                    get() = log.size

                override fun isEmpty(): Boolean = log.isEmpty()

                override fun iterator(): Iterator<U> {
                    val iter = log.iterator()
                    return object : Iterator<U> {
                        override fun hasNext(): Boolean {
                            return iter.hasNext()
                        }

                        override fun next(): U {
                            return f(iter.next())
                        }
                    }
                }
            }
        }
    }
}