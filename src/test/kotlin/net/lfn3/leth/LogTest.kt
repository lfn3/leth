package net.lfn3.leth

import net.lfn3.leth.unsafe.InMemoryPartitionedLog
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Disabled
import java.lang.IllegalArgumentException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.fail

@Disabled
open class LogTest(private val ctor: () -> Log<Pair<Long, Long>>) {
    @Test
    fun `The log returns a single recorded entry`() {
        val log = ctor.invoke()

        val entry = Pair(6L, 7L)
        log.record(entry)

        assertEquals(entry, log.head())
    }

    @Test
    fun `The log allows updates`() {
        val log = ctor.invoke()

        val entry = Pair(12L, 3L)
        val seq = log.record(entry)

        log.update({ seq }, { Pair(it.first, it.second + 1)})

        val updatedEntry = Pair(12L, 4L)
        assertEquals(updatedEntry, log.head())

        val iter =  log.iterator()
        assertEquals(entry, iter.next())
        assertEquals(updatedEntry, iter.next())
        assertFalse(iter.hasNext())
    }

    @Test
    fun `The log should throw on updates for unknown sequences`() {
        val log = ctor.invoke()

        val entry = Pair(12L, 3L)
        val seq = log.record(entry)
        //Should fail since we don't have this sequence, yet.
        assertThrows(Exception::class.java) { log.update({ seq + 1 }, { Pair(it.first, it.second + 1)}) }

        assertEquals(1, log.size)
        assertEquals(entry, log.get(seq))
    }

    @Test
    fun `Should see updates in tail`() {
        val log = ctor.invoke()
        val counter = AtomicInteger(0)

        log.tail { counter.incrementAndGet() }

        val entry = Pair(12L, 3L)
        val seq = log.record(entry)

        assertEquals(1, counter.get())

        log.update({ seq }, { Pair(it.first, it.second + 1)})

        assertEquals(2, counter.get())
    }

    @Test
    fun `Log should ignore updates to same value`() {
        val log = ctor.invoke()

        val entry = Pair(12L, 3L)
        val seq = log.record(entry)
        log.tail { fail("Should not have written back identical value") }

        log.update({ seq }, { Pair(it.first, it.second)})

        assertEquals(1, log.size)
    }

    @Test
    fun `Can iterate everything added to a log`() {
        val log = ctor.invoke()

        val first = Pair(6L, 7L)
        val second = Pair(7L, 7L)
        val third = Pair(9L, 7L)
        log.batchRecord(first, second, third)

        val iter = log.iterator()

        assertEquals(first, iter.next())
        assertEquals(second, iter.next())
        assertEquals(third, iter.next())
    }

    @Test
    fun `Iterator should not see newly added items`() {
        val log = ctor.invoke()

        val first = Pair(6L, 7L)
        log.record(first)

        val iter =  log.iterator()

        assertEquals(first, iter.next())
        log.record(Pair(6L, 7L))

        assertFalse(iter.hasNext())
    }

    @Test
    fun `Should be able to partition log`() {
        val log = ctor.invoke()

        val first: Pair<Long, Long> = Pair(1, 2)
        log.record(first)

        val indexed = InMemoryPartitionedLog(log) { it.first }

        assertEquals(first, indexed.head(1))
    }

    @Test
    fun `Should see new entries in partitioned log`() {
        val log = ctor.invoke()
        val indexed = InMemoryPartitionedLog(log) { it.first }

        val entry: Pair<Long, Long> = Pair(5, 6)
        log.record(entry)

        assertEquals(entry, log.head())
        assertEquals(entry, indexed.head(5))
    }

    @Test
    fun `Can tail log`() {
        val log = ctor.invoke()

        val counter = AtomicInteger(0)

        log.tail { counter.incrementAndGet() }

        log.record(Pair(5L, 12L))
        assertEquals(1, counter.get())
    }

    @Test
    fun `Can tail from a position in the log`() {
        val log = ctor.invoke()

        val counter = AtomicInteger(0)
        log.record(Pair(5L, 12L))

        log.tail(1) { counter.incrementAndGet() }

        log.record(Pair(5L, 12L))
        assertEquals(1, counter.get())
    }

    @Test
    fun `Can tail only updates using size`() {
        val log = ctor.invoke()
        val counter = AtomicInteger(0)


        val entry = Pair(12L, 3L)
        val seq = log.record(entry)

        log.tail(log.size) { counter.incrementAndGet() }

        assertEquals(0, counter.get())

        log.update({ seq }, { Pair(it.first, it.second + 1)})

        assertEquals(1, counter.get())
    }

    @Test
    fun `Observer that throws should not effect other observers`() {
        val log = ctor.invoke()

        val counter = AtomicInteger(0)

        log.tail { throw IllegalStateException() }
        log.tail { counter.incrementAndGet() }

        log.record(Pair(5L, 12L))
        assertEquals(1, counter.get())
    }
}
