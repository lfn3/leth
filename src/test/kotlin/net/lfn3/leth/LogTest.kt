package net.lfn3.leth

import net.lfn3.leth.unsafe.InMemoryPartitionedLog
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Nested
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.stream.LongStream
import kotlin.test.*

abstract class LogTest(private val ctor: () -> Log<Pair<Long, Long>>) {
    @Test
    fun `The log returns a single recorded entry`() {
        val log = ctor.invoke()

        val entry = Pair(6L, 7L)
        val seq = log.record(entry)
        val (queriedSeq, queriedVal) = log.headWithSeq()!!

        assertEquals(entry, queriedVal)
        assertEquals(seq, queriedSeq)
        assertEquals(1, log.size)
    }

    @Test
    fun `Can iterate everything added to a log`() {
        val log = ctor.invoke()

        val first = Pair(6L, 7L)
        val second = Pair(7L, 7L)
        val third = Pair(9L, 7L)
        log.batchRecord(arrayListOf(first, second, third))

        assertEquals(3, log.size)
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

        val iter = log.iterator()

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
    fun `Can map over log`() {
        val log = ctor.invoke()
        val mapped = LogReader.map(log) { it.first + it.second }
        val counter = AtomicLong()

        assertNull(mapped.head())

        log.record(Pair(3, 4))

        mapped.tail { counter.addAndGet(it) }

        assertEquals(7, counter.get())
        assertEquals(7, mapped.head())

        log.record(Pair(2, 1))

        assertEquals(10, counter.get())
        assertEquals(3, mapped.head())
    }

    @Nested
    inner class Batch {
        @Test
        fun `Should be able to batch insert`() {
            val log = ctor.invoke()

            val toInsert: Collection<Pair<Long, Long>> =
                LongStream.range(1, 100).mapToObj { Pair(1.toLong(), it) }.collect(Collectors.toList())
            log.batchRecord(toInsert)

            val iter = toInsert.iterator()
            log.tail { assertEquals(iter.next(), it) }
        }

        @Test
        fun `Batch insert should fire observers`() {
            val log = ctor.invoke()

            val counter = AtomicInteger(0)
            val toInsert = 100
            val entries =
                IntStream.range(0, toInsert).mapToObj { Pair(1.toLong(), it.toLong()) }.collect(Collectors.toSet())

            log.tail {
                counter.incrementAndGet()
                assert(entries.contains(it))
            }

            log.batchRecord(entries)

            assertEquals(toInsert, counter.get())
        }
    }

    @Nested
    inner class Tailing {
        @Test
        fun `Can tail log`() {
            val log = ctor.invoke()

            val counter = AtomicInteger(0)

            log.tail { counter.incrementAndGet() }

            log.record(Pair(5L, 12L))
            assertEquals(1, counter.get())
        }

        @Test
        fun `Should see updates in tail`() {
            val log = ctor.invoke()
            val counter = AtomicInteger(0)

            log.tail { counter.incrementAndGet() }

            val entry = Pair(12L, 3L)
            val seq = log.record(entry)

            assertEquals(1, counter.get())

            log.update({ seq }, { Pair(it.first, it.second + 1) })

            assertEquals(2, counter.get())
        }

        @Test
        fun `Should see existing values in tail`() {
            val log = ctor.invoke()

            val counter = AtomicInteger(0)
            log.record(Pair(5L, 12L))

            log.tail { counter.incrementAndGet() }

            log.record(Pair(5L, 13L))
            assertEquals(2, counter.get())
        }

        @Test
        fun `Can tail from a position in the log`() {
            val log = ctor.invoke()

            val counter = AtomicInteger(0)
            log.record(Pair(5L, 12L))
            val seq2 = log.record(Pair(5L, 12L))

            log.tail(seq2) { counter.incrementAndGet() }

            log.record(Pair(5L, 12L))
            assertEquals(2, counter.get())
        }

        @Test
        fun `Can tail only updates using seq + 1`() {
            val log = ctor.invoke()
            val counter = AtomicInteger(0)

            val entry = Pair(12L, 3L)
            val seq = log.record(entry)

            log.tail(seq + 1) { counter.incrementAndGet() }

            assertEquals(0, counter.get())

            log.update({ seq }, { Pair(it.first, it.second + 1) })

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

        @Test
        fun `Observer that throws should not effect other observers when batch inserting`() {
            val log = ctor.invoke()

            val counter = AtomicInteger(0)
            val toInsert = 100
            val entries =
                IntStream.range(0, toInsert).mapToObj { Pair(1.toLong(), it.toLong()) }.collect(Collectors.toSet())

            log.tail { throw IllegalStateException() }
            log.tail { counter.incrementAndGet() }

            log.batchRecord(entries)
            assertEquals(toInsert, counter.get())
        }
    }

    @Nested
    inner class Updates {
        @Test
        fun `The log allows updates`() {
            val log = ctor.invoke()

            val entry = Pair(12L, 3L)
            val seq = log.record(entry)

            log.update({ seq }, { Pair(it.first, it.second + 1) })

            val updatedEntry = Pair(12L, 4L)
            assertEquals(updatedEntry, log.head())
            assertEquals(2, log.size)

            val iter = log.iterator()
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
            assertThrows(Exception::class.java) { log.update({ seq + 1 }, { Pair(it.first, it.second + 1) }) }

            assertEquals(1, log.size)
            assertEquals(entry, log.get(seq))
        }

        @Test
        fun `Log should ignore updates to same value`() {
            val log = ctor.invoke()

            val entry = Pair(12L, 3L)
            val seq = log.record(entry)
            log.tail(seq + 1) { fail("Should not have written back identical value") }

            log.update({ seq }, { Pair(it.first, it.second) })

            assertEquals(1, log.size)
        }

        @Test
        fun `Should only apply a single conflicting update`() {
            val log = ctor.invoke()

            val seq = log.record(Pair(5, 1))

            log.update({ seq }, { Pair(it.first, it.second + 1) })
            assertThrows(java.lang.Exception::class.java, { log.update({ seq }, { Pair(it.first, it.second + 1) }) })

            assertEquals(2, log.head()!!.second)
        }

        @Test
        fun `Should only retry updates 5 times`() {
            val log = ctor.invoke()

            val counter = AtomicLong(0)

            log.record(Pair(0, 0))

            LongStream.range(0, 6).forEach {
                log.update({ log.headWithSeq()!!.first }, { Pair(it.first, it.second + 1) })
            }

            assertThrows(java.lang.Exception::class.java) {
                log.update({
                    val nextSeq = counter.incrementAndGet()
                    if (5 < nextSeq) {
                        fail("More than 5 attempts to get sequence")
                    }
                    nextSeq
                }, { Pair(it.first, it.second + 1) })
            }

            assertEquals(5, counter.get())
        }
    }
}
