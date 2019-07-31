package net.lfn3.leth.jooq

import net.lfn3.leth.Log
import net.lfn3.leth.LogReader
import net.lfn3.leth.LogWriter
import org.jooq.DSLContext
import org.jooq.TableRecord
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import java.sql.SQLException

class DatabaseBackedLog<T, R : TableRecord<R>>(
    private val logWriterMappings: LogWriterMappings<T, R>,
    private val dslProvider: () -> DSLContext,
    private val reader: DatabaseBackedLogReader<T, R> = DatabaseBackedLogReader(logWriterMappings.asReadonly(), dslProvider = dslProvider)
) : Log<T>, LogWriter<T>, LogReader<T> by reader {
    // I know what you're thinking. Why is this here?
    // We want to maintain the invariant that there aren't any gaps in the log ids
    // Postgres makes this somewhat tricky, since it does not have transactional rollbacks around sequences, so you can pull
    // a value from a sequence, conflict somewhere and then drop that value on the floor.
    // This would make us sad.
    // So, instead we maintain our own sequence in memory, and optimistically insert.
    // This will, of course, create problems if there's multiple writers.
    @Volatile
    private var hwm : Long? = null

    override fun record(entry: T): Long {
        val dbr = logWriterMappings.toRecord.invoke(entry)

        val seq = executeInsert(dbr)

        reader.notify(seq, entry)

        return seq
    }

    private fun executeInsert(dbr: R): Long {
        dslProvider().use { dsl ->
            val nextHwm = getHwm(dsl) + 1

            dbr[logWriterMappings.sequenceField] = nextHwm
            //TODO: back off and retry when the seq conflicts (how do we tell it's that field that conflicted?)
            dsl.insertInto(logWriterMappings.table)
                .set(dbr)
                .execute()

            hwm = nextHwm
            return nextHwm
        }
    }

    private fun getHwm(dsl: DSLContext) : Long {
        return if (hwm == null) {
            dsl.select(DSL.max(logWriterMappings.sequenceField))
                .from(logWriterMappings.table)
                .fetchOne()[DSL.max(logWriterMappings.sequenceField)] ?: 0
        } else {
            hwm!!
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