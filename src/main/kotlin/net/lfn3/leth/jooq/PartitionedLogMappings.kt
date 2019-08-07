package net.lfn3.leth.jooq

import org.jooq.*

data class PartitionedLogMappings<K, V, F, R : Record> (
    val table: Table<R>,
    val fromRecord: (R) -> V,
    val sequenceField: TableField<R, Long>,

    val byField: TableField<R, F>,
    val keyToDatabaseValue: (K) -> F,
    val extractKey: (V) -> K
) {
    fun asReadOnlyLogMappings(filter: Condition, inProcessFilter: (V) -> Boolean) : ReadOnlyLogMappings<V, R> {
        return asReadOnlyLogMappings(filter, inProcessFilter, fromRecord)
    }
    fun <T> asReadOnlyLogMappings(filter: Condition, inProcessFilter: (T) -> Boolean, fromRecord: (R) -> T) : ReadOnlyLogMappings<T, R> {
        return ReadOnlyLogMappings(table, fromRecord, sequenceField, filter, inProcessFilter)
    }
}