package net.lfn3.leth.jooq

import org.jooq.Record
import java.time.Duration
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean

class ThreadBackedLogPoller(private val threadFactory: ThreadFactory,
                            private val log : DatabaseBackedLogReader<Any, Record>,
                            private val betweenPolls: Duration = Duration.ofMillis(500),
                            private val interruptThreadIfNotStoppedBy: Duration = betweenPolls.multipliedBy(5)) : Runnable {
    private lateinit var thread : Thread
    private val stop : AtomicBoolean = AtomicBoolean(false)

    override fun run() {
        thread = threadFactory.newThread(this::pollLoop);
    }

    fun stop() {
        stop.set(true)
        thread.join(interruptThreadIfNotStoppedBy.toMillis())
        if (thread.isAlive) {
            thread.interrupt()
        }
    }

    fun pollLoop() {
        do {
            log.checkDatabase()
            Thread.sleep(betweenPolls.toMillis())
        } while (!stop.get())
    }
}