package net.lfn3.leth.jooq

import org.jooq.DSLContext
import org.jooq.TableRecord

internal class RecordDatabaseOperation<T, R : TableRecord<R>>(val record: R, private val logWriterMappings: LogWriterMappings<T, R>) : DatabaseOperation<R>() {

    override fun setSeq(seq: Long) {
        super.setSeq(seq)
        record.set(logWriterMappings.sequenceField, seq)
    }

    override fun doOperation(dsl : DSLContext): R {
        dsl.insertInto(logWriterMappings.table)
            .set(record)
            .execute()
        return record
    }
}