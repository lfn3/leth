package net.lfn3.leth.jooq

import net.lfn3.leth.*
import net.lfn3.leth.jooq.tables.Logged
import net.lfn3.leth.jooq.tables.records.LoggedRecord
import kotlin.test.Test
import org.junit.jupiter.api.Nested
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

class DatabaseBackedLogTest {
    private val tableDescriptor = LogWriterMappings(
        Logged.LOGGED,
        { Pair(it.valueOne, it.valueTwo) },
        Logged.LOGGED.LOG_ID,
        {
            val rec = LoggedRecord()
            rec.valueOne = it.first
            rec.valueTwo = it.second
            rec
        },
        Logged.LOGGED.ANCESTOR_ID
    )

    private fun makeLog() : Log<Pair<Long, Long>> {
        return CleanDatabaseBackedLog(tableDescriptor)
    }

    @Nested
    inner class Plain : LogTest(this::makeLog)

    @Nested
    inner class Concurrent : ConcurrentLogTest(this::makeLog)

    @Test
    fun `Should see database updates outside of log in head`() {
        val log = CleanDatabaseBackedLog(tableDescriptor)

        val record = LoggedRecord()
        record.valueOne = 5
        record.valueTwo = 12

        log.dslProvider().use { dsl ->
            dsl.insertInto(tableDescriptor.table)
                .set(record)
                .execute()
        }

        val head = log.head()

        assertEquals(5, head!!.first)
        assertEquals(12, head.second)
    }

    @Test
    fun `Should see database updates outside of log in tail`() {
        val log = CleanDatabaseBackedLog(tableDescriptor)

        val record = LoggedRecord()
        record.valueOne = 5
        record.valueTwo = 12

        log.dslProvider().use { dsl ->
            dsl.insertInto(tableDescriptor.table)
                .set(record)
                .execute()
        }

        val counter = AtomicInteger()

        //Normally you'd have to attach a ThreadBackedLogPoller to make this happen
        log.log.reader.checkDatabase()

        log.tail { counter.incrementAndGet() }

        assertEquals(1, counter.get())
    }

    @Test
    fun `Should see database updates outside of log pushed to tail`() {
        val log = CleanDatabaseBackedLog(tableDescriptor)

        val record = LoggedRecord()
        record.valueOne = 5
        record.valueTwo = 12

        val counter = AtomicInteger()
        log.tail { counter.incrementAndGet() }

        log.dslProvider().use { dsl ->
            dsl.insertInto(tableDescriptor.table)
                .set(record)
                .execute()
        }

        assertEquals(1, counter.get())
    }

    @Test
    fun `Observer that throws should not effect other observers when there is a missed update`() {
        // Attach throwing observer and counting observer.
        // Do an direct insert, then a regular insert
        // Make sure we see the update in the counting observer
        TODO()
    }
}