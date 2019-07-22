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
    val filter: Condition = DSL.trueCondition()
)