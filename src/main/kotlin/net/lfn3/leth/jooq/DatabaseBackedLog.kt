package net.lfn3.leth.jooq

import net.lfn3.leth.Log
import net.lfn3.leth.LogReader
import net.lfn3.leth.LogWriter
import net.lfn3.leth.Notifier
import org.jooq.DSLContext
import org.jooq.TableRecord
import org.jooq.exception.DataAccessException

class DatabaseBackedLog<T, R : TableRecord<R>>(
    private val logWriterMappings: LogWriterMappings<T, R>,
    private val dslProvider: () -> DSLContext,
    //Visible for testing
    val reader: DatabaseBackedLogReader<T, R> = DatabaseBackedLogReader(
        logWriterMappings.asReadonly(),
        dslProvider = dslProvider
    ),
    private val writer: LogWriter<T> = Notifier(DatabaseBackedLogWriter(
        logWriterMappings,
        dslProvider
    ), { seq, v ->  reader.notify(seq, v) })
) : Log<T>, LogWriter<T> by writer, LogReader<T> by reader