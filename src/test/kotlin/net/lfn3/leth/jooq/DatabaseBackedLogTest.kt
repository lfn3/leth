package net.lfn3.leth.jooq

import net.lfn3.leth.*
import net.lfn3.leth.jooq.tables.Logged
import net.lfn3.leth.jooq.tables.records.LoggedRecord
import org.jooq.TableRecord
import org.jooq.impl.DSL
import java.sql.Connection

class DatabaseBackedLogTest : LogTest({
    val tableDescriptor = LogWriterMappings(
        Logged.LOGGED,
        { Pair(it.valueOne, it.valueTwo) },
        Logged.LOGGED.LOG_ID,
        {
            val rec = LoggedRecord()
            rec.valueOne = it.first
            rec.valueTwo = it.second
            rec
        },
        Logged.LOGGED.ANCESTOR_ID,
        false
    )

    CleanDatabaseBackedLog(tableDescriptor)
}) {
    class CleanDatabaseBackedLog<T, R : TableRecord<R>>(
        logWriterMappings: LogWriterMappings<T, R>,
        private val jdbcUrl : String = DEFAULT_JDBC_URL + System.nanoTime(),
        private val log: DatabaseBackedLog<T, R> = DatabaseBackedLog(logWriterMappings, { DSL.using(jdbcUrl) })
    ) : Log<T> by log, AutoCloseable {

        private val conn: Connection = migrate(jdbcUrl) //We hang onto this so our database doesn't get torn down

        protected fun finalize() {
            conn.close()
        }

        override fun close() {
            conn.close()
        }

        override fun record(entry: T) = log.record(entry)

        override fun head(): T? = log.head()

        override fun get(sequence: Long): T? = log.get(sequence)

        override fun update(getSequence: () -> Long, fn: (T) -> T) = log.update(getSequence, fn)

        override fun tail(fn: (newEntry: T) -> Unit) = log.tail(fn)

        override val size: Int
            get() = log.size

        override fun isEmpty(): Boolean = log.isEmpty()

        override fun iterator(): Iterator<T> = log.iterator()
    }
}