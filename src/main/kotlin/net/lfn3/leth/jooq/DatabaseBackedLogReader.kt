package net.lfn3.leth.jooq

import net.lfn3.leth.LogReader
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SelectSeekStep1
import org.jooq.impl.DSL
import java.util.concurrent.CopyOnWriteArrayList

open class DatabaseBackedLogReader<T, R : Record>(private val readOnlyLogMappings: ReadOnlyLogMappings<T, R>,
                                                  private val dslProvider: () -> DSLContext
) : LogReader<T> {
    @Volatile
    private var hwm : Long = 0
    //TODO: these won't get notified if something gets changed when not linked to a writer.
    // Unless you attach a thread backed log poller
    private val observers : MutableList<(newEntry: T) -> Unit> = CopyOnWriteArrayList()

    private fun baseQuery(dsl : DSLContext, vararg whereClauses: Condition, desc: Boolean = true): SelectSeekStep1<R, Long> {
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
        //TODO: fetch from hwm and fire observers?
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
        //TODO: fetch from hwm and fire observers?
        dslProvider().use { dsl ->
            val record = baseQuery(dsl)
                .limit(1)
                .fetchAny()

            return when {
                record != null -> Pair(record.get(readOnlyLogMappings.sequenceField), readOnlyLogMappings.fromRecord(record))
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

    private fun fetch(fromInclusive: Long, toExclusive: Long, desc: Boolean = true) : Collection<T> {
        dslProvider().use { dsl ->
            return baseQuery(dsl,
                readOnlyLogMappings.sequenceField.greaterOrEqual(fromInclusive),
                readOnlyLogMappings.sequenceField.lessThan(toExclusive),
                desc = desc)
                .fetch()
                .map(readOnlyLogMappings.fromRecord)
        }
    }

    override fun tail(start: Long, fn: (T) -> Unit) {
        //TODO: how to handle concurrent inserts? Probably something involving the high water mark
        fetch(start, hwm, desc = false).forEach(fn)
        observers.add(fn)
    }

    override val size: Long
        get() {
            dslProvider().use { dsl ->
                return dsl.selectCount().from(readOnlyLogMappings.table).where(readOnlyLogMappings.filter).fetchOne().value1().toLong()
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
        if (seq <= hwm) { //Should have already seen these messages (somehow?!)
            return
        }

        if (seq != hwm + 1) {
            val missed = fetch(hwm + 1, seq)
            observers.forEach { ob -> missed.forEach { ob(it) }}
        }
        hwm = seq

        if (readOnlyLogMappings.inProcessFilter(new)) {
            observers.forEach { it(new) }
        }
    }

    private fun getDbHwm(): Long {
        return dslProvider().use { dsl ->
            dsl.select(DSL.max(readOnlyLogMappings.sequenceField))
                .from(readOnlyLogMappings.table)
                .fetchOne().get(0, Long::class.java)
        }
    }

    fun checkDatabase() {
        val dbHwm = getDbHwm()

        if (dbHwm > hwm) {
            notifyObservers(fetch(hwm, dbHwm + 1))
        }
    }

    private fun notifyObservers(vals: Collection<T>) {
        //TODO: split up observers to head and non-head. Just feed the last item to head observers
        observers.forEach { vals.forEach(it) }
        hwm += vals.size
    }

    fun notifyBatch(vals: Collection<T>) {
        // We know the hwm must have grown by at least `vals.size`. If it has grown by exactly `vals.size` then we can
        // fast path the observers, otherwise we have to go back to the db for the whole batch.
        val dbHwm = getDbHwm()
        if (hwm + vals.size == dbHwm) {
            notifyObservers(vals)
        } else {
            notifyObservers(fetch(hwm, dbHwm + 1))
        }
    }
}