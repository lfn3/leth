package net.lfn3.leth

interface LogReader<T> : Iterable<T> {
    fun head() : T?
    fun get(sequence: Long) : T?
    fun tail(fn : (newEntry: T) -> Unit)
    val size: Int
    fun isEmpty(): Boolean = size == 0

    companion object {
        fun <T, U> map(log: LogReader<T>, f : (T) -> U) : LogReader<U> {
            return object : LogReader<U> {
                override fun head(): U? {
                    val head = log.head() ?: return null
                    return f(head)
                }

                override fun get(sequence: Long): U? {
                    val v = log.get(sequence) ?: return null
                    return f(v)
                }

                override fun tail(fn: (newEntry: U) -> Unit) {
                    log.tail { fn(f(it)) }
                }

                override val size: Int
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