package net.lfn3.leth

import LogConsumer

interface LogFollower<T> {
    fun tail(logConsumer: LogConsumer<T>)
}