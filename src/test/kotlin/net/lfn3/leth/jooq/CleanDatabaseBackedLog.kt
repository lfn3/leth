package net.lfn3.leth.jooq

import net.lfn3.leth.DEFAULT_JDBC_URL
import net.lfn3.leth.Log
import net.lfn3.leth.migrate
import org.jooq.DSLContext
import org.jooq.TableRecord
import org.jooq.impl.DSL
import java.sql.Connection


class CleanDatabaseBackedLog<T, R : TableRecord<R>>(
    val logWriterMappings: LogWriterMappings<T, R>,
    private val jdbcUrl : String = DEFAULT_JDBC_URL + System.nanoTime(),
    val dslProvider : () -> DSLContext = { DSL.using(jdbcUrl) },
    private val log: DatabaseBackedLog<T, R> = DatabaseBackedLog(logWriterMappings, dslProvider)
) : Log<T> by log, AutoCloseable {

    private val conn: Connection = migrate(jdbcUrl) //We hang onto this so our database doesn't get torn down

    protected fun finalize() {
        conn.close()
    }

    override fun close() {
        conn.close()
    }
}