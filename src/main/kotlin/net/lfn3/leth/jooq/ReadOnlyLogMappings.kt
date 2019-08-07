package net.lfn3.leth.jooq

import org.jooq.Condition
import org.jooq.Record
import org.jooq.Table
import org.jooq.TableField
import org.jooq.impl.DSL

data class ReadOnlyLogMappings<T, R : Record>(
    val table: Table<R>,
    val fromRecord: (R) -> T,
    val sequenceField: TableField<R, Long>,
    //TODO: sanity check that the two filters match?
    val filter: Condition = DSL.trueCondition(),
    val inProcessFilter: (T) -> Boolean = { true }
) {
    fun <K, F> asPartitioned(
        byField: TableField<R, F>,
        keyToDatabaseValue: (K) -> F,
        extractKey: (T) -> K
    ): PartitionedLogMappings<K, T, F, R> {
        return PartitionedLogMappings(table, fromRecord, sequenceField, byField, keyToDatabaseValue, extractKey)
    }
}