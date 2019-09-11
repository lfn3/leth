package net.lfn3.leth

import org.junit.jupiter.api.Assertions
import java.lang.Exception
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.stream.Collectors
import java.util.stream.LongStream
import kotlin.test.assertEquals
import kotlin.test.Test

abstract class ConcurrentLogTest(private val ctor : () -> Log<Pair<Long, Long>>) {
    @Test
    fun `Should only apply a single conflicting update`() {
        val log = ctor.invoke()

        val seq = log.record(Pair(5, 1))

        log.update({ seq }, { Pair(it.first, it.second + 1) })
        Assertions.assertThrows(Exception::class.java, { log.update({ seq }, { Pair(it.first, it.second + 1) }) })

        assertEquals(2, log.head()!!.second)
    }

    @Test
    fun `Should only apply a single concurrent conflicting update`() {
        val log = ctor.invoke()

        val seq = log.record(Pair(5, 1))
        val semaphore = Semaphore(0)

        val thread = Thread {
            log.update({ seq }, {
                semaphore.acquire()
                Pair(it.first, it.second + 1)
            })
        }

        thread.start()
        try {
            log.update({ seq }, {
                semaphore.release()
                Pair(it.first, it.second + 1)
            })
        } catch (ignored : java.lang.Exception) {

        }

        thread.join()
        assertEquals(2, log.head()!!.second)
    }

    @Test
    fun `Should retry conflicting update`() {
        val log = ctor.invoke()

        val seq = log.record(Pair(5, 1))
        val semaphore = Semaphore(0)

        val thread = Thread {
            log.update({ log.headWithSeq()?.first ?: seq }, {
                semaphore.acquire()
                semaphore.release()
                Pair(it.first, it.second + 1)
            })
        }

        thread.start()

        log.update({ log.headWithSeq()?.first ?: seq }, {
            semaphore.release()
            Pair(it.first, it.second + 1)
        })

        thread.join()
        assertEquals(3, log.head()!!.second)
    }

    @Test
    fun `Batch insert should not conflict with other inserts`() {
        val log = ctor.invoke()

        val insertCount : Long = 10_000
        val toInsert : Collection<Pair<Long, Long>> = LongStream.range(0, insertCount).mapToObj { Pair(1.toLong(), it) }.collect(
            Collectors.toList())

        val thread = Thread {
            log.batchRecord(toInsert)
        }

        log.record(Pair(4, 60))
        thread.start()
        log.update({ log.headWithSeq()?.first ?: 0 }, {
            Pair(it.first, it.second + 1)
        })

        thread.join()
        assertEquals(insertCount + 2, log.size)
    }

    @Test
    fun `Tailing should not miss any entries`() {
        val log = ctor()

        val threadCount : Long = 2
        val insertCount : Long = 1_000
        val toInsert = LongStream.range(0, insertCount).boxed().collect(Collectors.toList())
        val expectedNext = ConcurrentHashMap<Long, Long>()
        LongStream.range(0, threadCount).forEach { expectedNext[it] = 0; }

        // I think the problem here is that notify (on the read side) is being called from multiple different threads.
        // Really, that notification needs to be put in queue or something and processed on another thread.
        // Or, every operation dealing with the hwm needs to be a CAS style operation?

        log.tail {
            assertEquals(expectedNext[it.first], it.second)
            expectedNext[it.first] = it.second + 1
        }

        val threads : List<Thread> = LongStream.range(0, threadCount)
            .mapToObj { l1 -> Thread { toInsert.forEach { l2 -> log.record(Pair(l1, l2)); } } }
            .collect(Collectors.toList())

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        LongStream.range(0, threadCount).forEach { expectedNext[it] = 0; }

        log.tail {
            assertEquals(expectedNext[it.first], it.second)
            expectedNext[it.first] = it.second + 1
        }

        assertEquals(threadCount * insertCount, log.size)
    }

    @Test
    fun `Interleaved batch inserts should be tailed correctly`() {
        TODO("Have multiple threads batch insert, check everything shows up in the `tail`, " +
                "and with correct (= to the database) ordering")
    }
}