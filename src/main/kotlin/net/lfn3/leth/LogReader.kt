package net.lfn3.leth

interface LogReader<T> {
    fun head() : T?
    fun headWithSeq() : Pair<Long, T>?
    fun get(sequence: Long) : T? {
        val iter = getBatch(sequence, 1).iterator()
        return if (iter.hasNext()) {
            iter.next()
        } else {
            null
        }
    }
    fun getBatch(fromSequence: Long, maxSize: Long) : Iterable<T>
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

                override fun getBatch(fromSequence: Long, maxSize: Long): Iterable<U> {
                    return log.getBatch(fromSequence, maxSize).map(f)
                }

                override val size: Long
                    get() = log.size

                override fun isEmpty(): Boolean = log.isEmpty()
            }
        }
    }
}