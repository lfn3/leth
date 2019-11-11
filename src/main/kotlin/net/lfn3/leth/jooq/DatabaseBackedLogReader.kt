package net.lfn3.leth.jooq

import net.lfn3.leth.LogReader
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SelectSeekStep1

open class DatabaseBackedLogReader<T, R : Record>(
    private val readOnlyLogMappings: ReadOnlyLogMappings<T, R>,
    private val dslProvider: () -> DSLContext
) : LogReader<T> {
    private fun baseQuery(
        dsl: DSLContext,
        vararg whereClauses: Condition,
        desc: Boolean = true
    ): SelectSeekStep1<R, Long> {
        var query = dsl.selectFrom(readOnlyLogMappings.table)
            .where(readOnlyLogMappings.filter)

        whereClauses.forEach { query = query.and(it) }

        return if (desc) {
            query.orderBy(readOnlyLogMappings.sequenceField.desc())
        } else {
            query.orderBy(readOnlyLogMappings.sequenceField.asc())
        }
    }

    override fun head(): T? {
        dslProvider().use { dsl ->
            val record = baseQuery(dsl)
                .limit(1)
                .fetchAny()

            return when {
                record != null -> readOnlyLogMappings.fromRecord(record)
                else -> null
            }
        }
    }

    override fun headWithSeq(): Pair<Long, T>? {
        dslProvider().use { dsl ->
            val record = baseQuery(dsl)
                .limit(1)
                .fetchAny()

            return when {
                record != null -> Pair(
                    record.get(readOnlyLogMappings.sequenceField),
                    readOnlyLogMappings.fromRecord(record)
                )
                else -> null
            }
        }
    }

    override fun get(sequence: Long): T? {
        dslProvider().use { dsl ->
            val record = baseQuery(dsl, readOnlyLogMappings.sequenceField.eq(sequence))
                .limit(1)
                .fetchAny()

            return when {
                record != null -> readOnlyLogMappings.fromRecord(record)
                else -> null
            }
        }
    }

    override fun getBatch(fromSequence: Long, maxSize: Long): Iterable<T> {
        require(maxSize <= Int.MAX_VALUE) { "maxSize may not be greater than " + Int.MAX_VALUE }

        dslProvider().use { dsl ->
            return baseQuery(
                dsl,
                readOnlyLogMappings.sequenceField.greaterOrEqual(fromSequence),
                desc = false
            ).limit(maxSize.toInt())
                .fetch()
                .map(readOnlyLogMappings.fromRecord)
        }
    }

    override val size: Long
        get() {
            dslProvider().use { dsl ->
                return dsl.selectCount().from(readOnlyLogMappings.table).where(readOnlyLogMappings.filter).fetchOne()
                    .value1().toLong()
            }
        }
}