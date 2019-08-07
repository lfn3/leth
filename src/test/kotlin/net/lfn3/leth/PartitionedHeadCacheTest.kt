package net.lfn3.leth

import net.lfn3.leth.caches.PartitionedHeadCache
import net.lfn3.leth.unsafe.InMemoryLog
import net.lfn3.leth.unsafe.InMemoryPartitionedLog
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class PartitionedHeadCacheTest {
    @Test
    fun `should be empty on start`() {
        val log = InMemoryLog<Long>()
        val partitioned = InMemoryPartitionedLog(log, Long::toString)
        val cache = PartitionedHeadCache(partitioned)

        assertNull(cache.get("0"))
    }

    @Test
    fun `should contain first inserted value`() {
        val log = InMemoryLog<Long>()
        val partitioned = InMemoryPartitionedLog(log, Long::toString)
        val cache = PartitionedHeadCache(partitioned)

        log.record(1)
        assertEquals(1, cache.get("1"))
    }

    @Test
    fun `should contain value inserted before cache construction`() {
        val log = InMemoryLog<Long>()
        log.record(1)
        val partitioned = InMemoryPartitionedLog(log, Long::toString)
        val cache = PartitionedHeadCache(partitioned)

        assertEquals(1, cache.get("1"))
    }

    @Test
    fun `should be able to build from already partitioned log`() {
        val log = InMemoryLog<Long>()
        val partitioned = InMemoryPartitionedLog(log, Long::toString)
        val cache = PartitionedHeadCache(partitioned)

        log.record(7)

        assertEquals(7, cache.get("7"))
    }

    @Test
    fun `should see all existing partitions from log`() {
        val log = InMemoryLog<Pair<Long, Long>>()
        log.record(Pair(1, 2))
        log.record(Pair(2, 7))
        log.record(Pair(2, 8))
        val partitioned = InMemoryPartitionedLog(log) { it.first }
        val cache = PartitionedHeadCache(partitioned)

        assertEquals(Pair<Long, Long>(2, 8), cache.get(2))
        assertEquals(Pair<Long, Long>(1, 2), cache.get(1))
    }
}