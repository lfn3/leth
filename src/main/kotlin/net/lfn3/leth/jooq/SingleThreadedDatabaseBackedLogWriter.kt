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

//TODO: think we should extract a worker class
class SingleThreadedDatabaseBackedLogWriter<T, R : TableRecord<R>>(
    private val logWriterMappings : LogWriterMappings<T, R>,
    private val dslProvider: () -> DSLContext,
    private val threadFactory : ThreadFactory = ThreadFactory { Thread(it, "SingleThreadedDatabaseBackedLogWriter for " + logWriterMappings.table.name) }
) : LogWriter<T> {
    private var seq : Long = NOT_SET
    private val queue : BlockingQueue<DatabaseOperation<R>> = ArrayBlockingQueue(1024)

    private var thread : Thread? = null
    @Volatile
    private var run = false

    @Synchronized
    fun start() {
        check(!run) { toString() + "has already been started" }
        run = true
        val thread = threadFactory.newThread {
            initSeq()
            while (run) {
                doWork()
            }
        }

        thread.start()

        this.thread = thread
    }

    @Synchronized
    fun stop() {
        check(run) { toString() + " has already been stopped" }
        run = false
    }

    fun getSeq() : Long {
        check(seq != NOT_SET) { "Init seq should have already been called" }
        return seq++
    }

    private fun initSeq() {
        if (seq == NOT_SET) {
            dslProvider().use { dsl ->
                seq = dsl.select(logWriterMappings.sequence.currval()).fetchOne()[0, Long::class.java]
            }
        }
    }

    fun rollbackSeq() {
        check(seq != NOT_SET) { "Seq has not been set, so we don't know how to roll back" }

        seq--
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
            op.setSeq(getSeq())
            op.run(dsl)
            if (op.result.isCompletedExceptionally) {
                op.setSeq(NOT_SET)
                rollbackSeq()
            }
        }
    }
}
