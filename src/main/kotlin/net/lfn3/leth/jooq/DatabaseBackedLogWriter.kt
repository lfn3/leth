package net.lfn3.leth.jooq

import net.lfn3.leth.LogWriter
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.TableRecord
import org.jooq.exception.DataAccessException
import java.lang.IllegalStateException

class DatabaseBackedLogWriter<T, R : TableRecord<R>>(
    private val logWriterMappings: LogWriterMappings<T, R>,
    private val dslProvider: () -> DSLContext
) : LogWriter<T> {

    companion object {
        private const val MAX_RETRIES = 5
        private const val NO_LAST_SEQ: Long = -1
    }

    private fun executeInsert(dbr: R): Long {
        return dslProvider().use { dsl ->
            dsl.insertInto(logWriterMappings.table)
                .set(dbr)
                .returning(logWriterMappings.sequenceField)
                .fetchOne()
                .get(logWriterMappings.sequenceField)
        }
    }

    override fun record(entry: T): Long = executeInsert(logWriterMappings.toRecord(entry))

    @Throws(DataAccessException::class)
    override fun update(getSequence: () -> Long, fn: (T) -> T): Pair<Long, T> {
        var lastSeq: Long = NO_LAST_SEQ
        var lastEx: DataAccessException? = null
        for (i in 1..MAX_RETRIES) {
            val seq = getSequence()
            if (seq == lastSeq && lastEx != null) {
                throw lastEx
            }

            try {
                return dslProvider().use<DSLContext, Pair<Long, T>> { dsl ->
                    val record = dsl.selectFrom(logWriterMappings.table)
                        .where(logWriterMappings.sequenceField.eq(seq))
                        .forUpdate()
                        .fetchOne() ?: throw IllegalArgumentException("No log entry exists for sequence $seq")

                    val toUpdate = logWriterMappings.fromRecord(record)

                    val updated = fn(toUpdate)

                    if (updated == toUpdate) {
                        return Pair(seq, toUpdate)
                    }

                    val updatedRecord = logWriterMappings.toRecord(updated)

                    updatedRecord.set(logWriterMappings.ancestorSequenceField, seq)

                    val query = dsl.insertInto(logWriterMappings.table)
                        .set(updatedRecord)

                    val resultingSeq = query.returning(logWriterMappings.sequenceField)
                        .fetchOne()
                        .get(logWriterMappings.sequenceField)

                    Pair(resultingSeq, updated)
                }
            } catch (e: DataAccessException) {
                //TODO only retry on a constraint violation
                when {
                    lastSeq == NO_LAST_SEQ -> lastSeq = seq
                    i == MAX_RETRIES -> throw e
                    lastSeq != seq -> lastSeq = seq
                }
                lastEx = e
                // TODO Should we back off a little bit?
                // Thread.sleep((i * 10).toLong())
            }
        }
        throw IllegalStateException("Should not have been able to reach this point")
    }

    override fun batchRecord(entries: Collection<T>): Collection<Long> {
        return dslProvider().use { dsl ->
            val seqs: Collection<Long>
            if (dsl.dialect().family() == SQLDialect.POSTGRES) {
                seqs = dsl.select(logWriterMappings.sequence.nextval())
                    .from("generate_series(1, ?)", entries.size)
                    .fetchInto(Long::class.java)
            } else {
                TODO()
            }
            val seqIter = seqs.iterator()
            dsl.batchInsert(entries
                .map(logWriterMappings.toRecord)
                .map { it.set(logWriterMappings.sequenceField, seqIter.next()); it }).execute()

            seqs
        }
    }
}