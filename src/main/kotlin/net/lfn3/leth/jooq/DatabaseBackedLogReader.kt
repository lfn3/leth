package net.lfn3.leth.jooq

import net.lfn3.leth.LogReader
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SelectSeekStep1
import org.jooq.impl.DSL
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

open class DatabaseBackedLogReader<T, R : Record>(
    private val readOnlyLogMappings: ReadOnlyLogMappings<T, R>,
    private val dslProvider: () -> DSLContext
) : LogReader<T> {
    private var seenHwm: AtomicLong = AtomicLong(0)
    //TODO: these won't get notified if something gets changed when not linked to a writer.
    // Unless you attach a thread backed log poller
    private val observers: MutableList<(newEntry: T) -> Unit> = CopyOnWriteArrayList()

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

    private fun fetch(fromInclusive: Long, toExclusive: Long, desc: Boolean = true): Collection<T> {
        dslProvider().use { dsl ->
            return baseQuery(
                dsl,
                readOnlyLogMappings.sequenceField.greaterOrEqual(fromInclusive),
                readOnlyLogMappings.sequenceField.lessThan(toExclusive),
                desc = desc
            ).fetch()
                .map(readOnlyLogMappings.fromRecord)
        }
    }

    override fun tail(from: Long, fn: (T) -> Unit) {
        //TODO: how to handle concurrent inserts? Probably something involving the high water mark
        // note what's here now isn't sufficient
        synchronized(observers) {
            val fromDb = fetch(from, seenHwm.get() + 1, desc = false)
            fromDb.forEach(fn)
            observers.add(fn)
        }
    }

    override val size: Long
        get() {
            dslProvider().use { dsl ->
                return dsl.selectCount().from(readOnlyLogMappings.table).where(readOnlyLogMappings.filter).fetchOne()
                    .value1().toLong()
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
    internal fun notify(seq: Long, new: T) {
        var updatesToFire : Collection<T>
        do {
            val capturedHwm = seenHwm.get()
            if (seq <= capturedHwm) {
                //Should have already seen this message thanks to another thread
                updatesToFire = Collections.emptyList()
                break
            } else if (seq == capturedHwm + 1) {
                //This is the next expected message
                updatesToFire = listOf(new)
            } else {
                //Worst case: our high water mark is behind the seq by more than 1. fetch to catch up.
                val fetched = fetch(capturedHwm, seq).toMutableList()
                fetched.add(new)
                updatesToFire = fetched
                //TODO: rather than going back to the database if we fall into this case more than once,
                // we could take a slice of the existing 'updatesToFire'. (Since our hwm is only allowed to move forwards)
                // Need to collect sequence numbers as well in order to make that doable.
            }
        } while (!seenHwm.compareAndSet(capturedHwm, seq))

        //TODO: shouldn't just call this - might get hit by multiple different threads,
        // which would cause messages to get delivered out of order
        notifyObservers(updatesToFire)
    }

    private fun getDbHwm(): Long {
        return dslProvider().use { dsl ->
            dsl.select(DSL.max(readOnlyLogMappings.sequenceField))
                .from(readOnlyLogMappings.table)
                .fetchOne().get(0, Long::class.java)
        }
    }

    fun checkDatabase() {
        var updatesToFire : Collection<T>
        do {
            val seq = getDbHwm()
            val capturedHwm = seenHwm.get()
            if (seq <= capturedHwm) {
                //Should have already seen this message thanks to another thread
                updatesToFire = Collections.emptyList()
                break
            } else {
                updatesToFire = fetch(capturedHwm, seq + 1)
                //TODO: rather than going back to the database if we fall into this case more than once,
                // we could take a slice of the existing 'updatesToFire'. (Since our hwm is only allowed to move forwards)
                // Need to collect sequence numbers as well in order to make that doable.
            }
        } while (!seenHwm.compareAndSet(capturedHwm, seq))

        //TODO: shouldn't just call this - might get hit by multiple different threads,
        // which would cause messages to get delivered out of order
        notifyObservers(updatesToFire)
    }

    private fun notifyObservers(vals: Collection<T>) {
        //TODO: split up observers to head and non-head. Just feed the last item to head observers
        observers.forEach {
            try {
                vals.forEach(it)
            } catch (e: Exception) {
                //TODO: allow wiring of an onException function here?
            }
        }
    }

    fun notifyBatch(vals: Collection<T>) {
        // We know the hwm must have grown by at least `vals.size`. If it has grown by exactly `vals.size` then we can
        // fast path the observers, otherwise we have to go back to the db for the whole batch.
        val dbHwm = getDbHwm()
        val hwm = this.seenHwm.get()
        if (hwm + vals.size == dbHwm && this.seenHwm.compareAndSet(hwm, dbHwm)) {
            notifyObservers(vals)
        } else {
            notifyObservers(fetch(hwm, dbHwm + 1)) //TODO this did something weird when outside of the else block?
        }
    }
}