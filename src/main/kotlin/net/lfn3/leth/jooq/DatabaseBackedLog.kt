package net.lfn3.leth.jooq

import net.lfn3.leth.Log
import net.lfn3.leth.LogReader
import net.lfn3.leth.LogWriter
import org.jooq.DSLContext
import org.jooq.TableRecord
import org.jooq.exception.DataAccessException
import java.sql.SQLException

class DatabaseBackedLog<T, R : TableRecord<R>>(
    private val logWriterMappings: LogWriterMappings<T, R>,
    private val dslProvider: () -> DSLContext,
    private val reader: DatabaseBackedLogReader<T, R> = DatabaseBackedLogReader(logWriterMappings.asReadonly(), dslProvider = dslProvider)
) : Log<T>, LogWriter<T>, LogReader<T> by reader {
    override fun record(entry: T): Long {
        val dbr = logWriterMappings.toRecord.invoke(entry)

        val seq = executeInsert(dbr)

        reader.notify(seq, entry)

        return seq
    }

    private fun executeInsert(dbr: R): Long {
        return dslProvider().use { dsl ->
            val query = dsl.insertInto(logWriterMappings.table)
                .set(dbr)

            if (logWriterMappings.supportsReturning) {
                query.returning(logWriterMappings.sequenceField)
                    .fetchOne()
                    .get(logWriterMappings.sequenceField)
            } else {
                query.execute()

                dsl.fetchValue("select scope_identity()") as Long
            }
        }
    }

    override fun update(getSequence: () -> Long, fn: (T) -> T) {
        val seq = getSequence()
        val toUpdate = get(seq) ?: throw IllegalArgumentException("No log entry exists for sequence $seq")
        val updated = fn(toUpdate)

        if (updated == toUpdate) {
            return
        }

        val updatedRecord = logWriterMappings.toRecord(updated)

        updatedRecord.set(logWriterMappings.ancestorSequenceField, seq)

        try {
            val insertedSeq = executeInsert(updatedRecord)
            reader.notify(insertedSeq, updated)
        } catch (e: DataAccessException) {
            e.getCause(SQLException::class.java)
            throw e
            //TODO: retry on conflict by re-getting seq
        }
    }
}