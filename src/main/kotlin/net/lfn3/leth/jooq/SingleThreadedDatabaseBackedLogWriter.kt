package net.lfn3.leth.jooq

import net.lfn3.leth.LogWriter
import net.lfn3.leth.jooq.Sequences.NOT_SET
import org.jooq.Block
import org.jooq.DSLContext
import org.jooq.TableRecord
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ThreadFactory
import kotlin.collections.ArrayList

class SingleThreadedDatabaseBackedLogWriter<T, R : TableRecord<R>>(
    private val logWriterMappings : LogWriterMappings<T, R>,
    private val dslProvider: () -> DSLContext,
    private val threadFactory : ThreadFactory = ThreadFactory { Thread("SingleThreadedDatabaseBackedLogWriter for " + logWriterMappings.table.name) }
) : LogWriter<T> {
    private var seq : Long = NOT_SET
    private val queue : BlockingQueue<DatabaseOperation<R>> = ArrayBlockingQueue(1024)

    @Volatile
    private var run = false

    @Synchronized
    fun start() {
        check(!run) { toString() + "has already been started" }
        run = true
        threadFactory.newThread {
            while (run) {
                doWork()
            }
        }
    }

    @Synchronized
    fun stop() {
        check(run) { toString() + " has already been stopped "}
        run = false
    }

    fun getSeq() : Long {
        if (seq == NOT_SET) {
            TODO("Pull from database")
        }

        return seq
    }

    @Throws(IllegalStateException::class)
    override fun record(entry: T): Long {
        val op = RecordDatabaseOperation(logWriterMappings.toRecord(entry), logWriterMappings)

        queue.add(op)

        op.result.get()

        return op.getSeq()
    }

    override fun update(getSequence: () -> Long, fn: (T) -> T): Pair<Long, T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun toString(): String = "SingleThreadedDatabaseBackedLogWriter for " + logWriterMappings.table.name

    fun doWork() {
        //TODO: how do we break out of this when we're stopping? (Think we push something through the queue?)
        val op = queue.take()

        dslProvider().use { dsl ->
            op.setSeq(seq++)
            op.run(dsl)
            if (op.result.isCompletedExceptionally) {
                op.setSeq(NOT_SET)
                seq--
            }
        }
    }
}
