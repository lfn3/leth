package net.lfn3.leth.jooq

import org.jooq.Sequence
import org.jooq.Table
import org.jooq.TableField
import org.jooq.TableRecord

data class LogWriterMappings<T, R : TableRecord<R>>(
    val table: Table<R>,
    val fromRecord: (R) -> T,
    val sequenceField: TableField<R, Long>,
    val sequence : Sequence<Long>,
    val toRecord: (T) -> R,
    val ancestorSequenceField: TableField<R, Long>
) {
    fun asReadonly(): ReadOnlyLogMappings<T, R> = ReadOnlyLogMappings(table, fromRecord, sequenceField)
}