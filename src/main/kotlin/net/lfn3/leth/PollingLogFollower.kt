package net.lfn3.leth

import LogConsumer
import java.time.Duration
import java.util.*
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.ScheduledExecutorService

class PollingLogFollower<T>(
    private val reader: LogReader<T>,
    private val schEx: ScheduledExecutorService,
    private val delay: Duration
) : LogFollower<T> {
    private val observers = CopyOnWriteArraySet<LogConsumer<T>>()

    override fun tail(logConsumer: LogConsumer<T>) {
        observers.add(logConsumer)
    }
}