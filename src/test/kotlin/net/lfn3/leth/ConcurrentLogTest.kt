package net.lfn3.leth

import org.junit.jupiter.api.Assertions
import java.lang.Exception
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
        TODO("Spin up threads to insert and repeatedly tail, make sure tail threads see all inserts at the end of" +
                "the test")
    }
}