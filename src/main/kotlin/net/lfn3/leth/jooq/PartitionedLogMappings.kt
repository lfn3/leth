package net.lfn3.leth.jooq

import org.jooq.Condition
import org.jooq.Table
import org.jooq.TableField
import org.jooq.TableRecord

data class PartitionedLogMappings<K, V, F, R : TableRecord<R>> (
    val table: Table<R>,
    val fromRecord: (R) -> V,
    val sequenceField: TableField<R, Long>,
    val byField: TableField<R, F>,
    val toDb: (K) -> F
) {
    fun asReadOnlyLogMappings(filter: Condition) : ReadOnlyLogMappings<V, R> {
        return ReadOnlyLogMappings(table, fromRecord, sequenceField, filter)
    }
}