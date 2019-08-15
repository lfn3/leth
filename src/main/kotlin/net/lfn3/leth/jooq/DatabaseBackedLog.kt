package net.lfn3.leth.jooq

import net.lfn3.leth.Log
import net.lfn3.leth.LogReader
import net.lfn3.leth.LogWriter
import org.jooq.DSLContext
import org.jooq.TableRecord
import org.jooq.exception.DataAccessException

class DatabaseBackedLog<T, R : TableRecord<R>>(
    private val logWriterMappings: LogWriterMappings<T, R>,
    private val dslProvider: () -> DSLContext,
    private val reader: DatabaseBackedLogReader<T, R> = DatabaseBackedLogReader(
        logWriterMappings.asReadonly(),
        dslProvider = dslProvider
    )
) : Log<T>, LogWriter<T>, LogReader<T> by reader {

    companion object {
        private const val MAX_RETRIES = 5
        private const val NO_LAST_SEQ: Long = -1
    }

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

            if (logWriterMappings.supportsReturning) { //TODO: should be able to use dialect
                query.returning(logWriterMappings.sequenceField)
                    .fetchOne()
                    .get(logWriterMappings.sequenceField)
            } else {
                query.execute()

                dsl.fetchValue("select scope_identity()") as Long
            }
        }
    }

    @Throws(DataAccessException::class)
    override fun update(getSequence: () -> Long, fn: (T) -> T) {
        var lastSeq: Long = NO_LAST_SEQ
        var lastEx : DataAccessException? = null
        for (i in 1..MAX_RETRIES) {
            val seq = getSequence()
            if (seq == lastSeq && lastEx != null) {
                throw lastEx
            }

            try {
                val (insertedSeq, updated) = dslProvider().use { dsl ->
                    val record = dsl.selectFrom(logWriterMappings.table)
                        .where(logWriterMappings.sequenceField.eq(seq))
                        .forUpdate()
                        .fetchOne() ?: throw IllegalArgumentException("No log entry exists for sequence $seq")

                    val toUpdate = logWriterMappings.fromRecord(record)

                    val updated = fn(toUpdate)

                    if (updated == toUpdate) {
                        return
                    }

                    val updatedRecord = logWriterMappings.toRecord(updated)

                    updatedRecord.set(logWriterMappings.ancestorSequenceField, seq)

                    val query = dsl.insertInto(logWriterMappings.table)
                        .set(updatedRecord)

                    if (logWriterMappings.supportsReturning) { //TODO: should be able to use dialect?
                        Pair(
                            query.returning(logWriterMappings.sequenceField)
                                .fetchOne()
                                .get(logWriterMappings.sequenceField), updated
                        )
                    } else {
                        query.execute()

                        Pair(dsl.fetchValue("select scope_identity()") as Long, updated)
                    }
                }
                reader.notify(insertedSeq, updated)

                return
            } catch (e: DataAccessException) {
                //TODO only retry on a constraint violation
                when {
                    lastSeq == NO_LAST_SEQ -> lastSeq = seq
                    i == MAX_RETRIES -> throw e
                    lastSeq != seq -> lastSeq = seq
                }
                lastEx = e
            }
        }
    }
}