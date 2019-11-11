package net.lfn3.leth.jooq

import net.lfn3.leth.*
import org.jooq.DSLContext
import org.jooq.TableRecord
import org.jooq.impl.DSL
import java.sql.Connection


class CleanDatabaseBackedLog<T, R : TableRecord<R>>(
    val logWriterMappings: LogWriterMappings<T, R>,
    private val jdbcUrl : String = DEFAULT_JDBC_URL + System.nanoTime(),
    val dslProvider : () -> DSLContext = { DSL.using(jdbcUrl) },
    val reader : DatabaseBackedLogReader<T, R> = DatabaseBackedLogReader(logWriterMappings.asReadonly(), dslProvider),
    val writer : SingleThreadedDatabaseBackedLogWriter<T, R> = SingleThreadedDatabaseBackedLogWriter(logWriterMappings, dslProvider),
    val notifier : Notifier<T> = Notifier(writer, { seq, t -> reader.notify(seq, t) }, { seqs, ts -> reader.notifyBatch(ts)}),
    val log: Log<T> = Log.from(reader, notifier)
) : Log<T> by log, AutoCloseable {

    private val conn: Connection = migrate(jdbcUrl) //We hang onto this so our database doesn't get torn down

    init {
        writer.start() //Normally you'd do this as part of application startup but this is just for tests, so eh.
    }

    protected fun finalize() {
        writer.stop()
        conn.close()
    }

    override fun close() {
        writer.stop()
        conn.close()
    }
}