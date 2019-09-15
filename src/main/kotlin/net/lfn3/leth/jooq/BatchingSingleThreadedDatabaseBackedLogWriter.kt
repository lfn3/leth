package net.lfn3.leth.jooq

import net.lfn3.leth.LogWriter
import net.lfn3.leth.jooq.Sequences.NOT_SET
import org.jooq.DSLContext
import org.jooq.TableRecord
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ThreadFactory

class BatchingSingleThreadedDatabaseBackedLogWriter<T, R : TableRecord<R>>(
    private val logWriterMappings : LogWriterMappings<T, R>,
    private val dslProvider: () -> DSLContext,
    private val threadFactory : ThreadFactory = ThreadFactory { Thread(name(logWriterMappings)) },
    private val batchSize : Int = 64,
    private val sleepMillis : Long = 32
) : LogWriter<T> {
    private var seq : Long = NOT_SET
    private val queue : BlockingQueue<DatabaseOperation<R>> = ArrayBlockingQueue(1024)

    @Volatile
    private var run = false

    companion object {
        fun <T, R : TableRecord<R>> name(logWriterMappings : LogWriterMappings<T, R>) : String =
            BatchingSingleThreadedDatabaseBackedLogWriter::class.java.simpleName + " for " + logWriterMappings.table.name
    }

    @Synchronized
    fun start() {
        check(!run) { toString() + "has already been started" }
        run = true
        threadFactory.newThread {
            while (run) {
                doWork()
                Thread.sleep(sleepMillis)
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

    override fun toString(): String = name(logWriterMappings)

    fun doWork() {
        val batch = ArrayList<DatabaseOperation<R>>(batchSize)
        queue.drainTo(batch, batchSize)

        dslProvider().use { dsl ->
            batch.forEach {
                it.setSeq(seq++)
                it.run(dsl)
                if (it.result.isCompletedExceptionally) {
                    it.setSeq(NOT_SET)
                    seq--
                }
            }
        }
    }
}
