package net.lfn3.leth.jooq

import net.lfn3.leth.LogReader
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SelectSeekStep1

open class DatabaseBackedLogReader<T, R : Record>(private val readOnlyLogMappings: ReadOnlyLogMappings<T, R>,
                                                  private val dslProvider: () -> DSLContext
) : LogReader<T> {
    private var hwm : Long = -1
    //TODO: these won't get notified if something gets changed when not linked to a writer.
    private val observers : MutableList<(newEntry: T) -> Unit> = ArrayList()

    private fun baseQuery(dsl : DSLContext, vararg whereClauses: Condition, desc: Boolean = true): SelectSeekStep1<R, Long> {
         var query = dsl.selectFrom(readOnlyLogMappings.table)
            .where(readOnlyLogMappings.filter)

        whereClauses.forEach { query = query.and(it) }

        return if (desc) {
            query.orderBy(readOnlyLogMappings.sequenceField.desc())
        } else {
            query.orderBy(readOnlyLogMappings.sequenceField)
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

    private fun fetch(fromInclusive: Long, toExclusive: Long) : Collection<T> {
        dslProvider().use { dsl ->
            return baseQuery(dsl,
                readOnlyLogMappings.sequenceField.greaterOrEqual(fromInclusive),
                readOnlyLogMappings.sequenceField.lessThan(toExclusive))
                .fetch()
                .map(readOnlyLogMappings.fromRecord)
        }
    }

    override fun tail(fn: (newEntry: T) -> Unit) {
        observers.add(fn)
    }

    override val size: Int
        get() {
            dslProvider().use { dsl ->
                return dsl.selectCount().from(readOnlyLogMappings.table).where(readOnlyLogMappings.filter).fetchOne().value1()
            }
        }

    override fun iterator(): Iterator<T> {
        dslProvider().use { dsl ->
            return baseQuery(dsl, desc = false)
                .fetch()
                .map(readOnlyLogMappings.fromRecord)
                .iterator()
        }
    }

    //This is used to fast path around on write
    //TODO: tests!
    internal fun notify(seq: Long, new : T) {
        if (seq != hwm + 1) {
            val missed = fetch(hwm + 1, seq)
            observers.forEach { ob -> missed.forEach { ob(it) }}
        } else {
            hwm = seq
        }
        //TODO: check this matches the filter?

        observers.forEach { it(new) }
    }
}