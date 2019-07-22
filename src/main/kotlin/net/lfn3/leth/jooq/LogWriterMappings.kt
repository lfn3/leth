package net.lfn3.leth.jooq

import org.jooq.Table
import org.jooq.TableField
import org.jooq.TableRecord

data class LogWriterMappings<T, R : TableRecord<R>> (
    val table: Table<R>,
    val fromRecord: (R) -> T,
    val sequenceField: TableField<R, Long>,
    val toRecord: (T) -> R,
    val ancestorSequenceField: TableField<R, Long>,
    val supportsReturning: Boolean = true
) {
    fun asReadonly() : ReadOnlyLogMappings<T, R> = ReadOnlyLogMappings(table, fromRecord, sequenceField)
}