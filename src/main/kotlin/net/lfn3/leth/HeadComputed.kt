package net.lfn3.leth

import LogConsumer

abstract class HeadComputed <T, U>(log: LogReader<T>,
                                   logFollower: LogFollower<T>,
                                   init: U,
                                   op: (acc : U, T) -> U) {
    protected var value : U = init

    init {
        val head = log.head()
        if (head != null) {
            value = op(init, head)
        }

        val logConsumer = object : LogConsumer<T> {
            override fun accept(t: Long, u: T) {
                val result = op(value, u)
                value = result
            }
        }
        logFollower.tail(logConsumer)
    }
}