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
        val cache = PartitionedHeadCache(log, { it.toString() }, { it })

        assertNull(cache.get("0"))
    }

    @Test
    fun `should contain first inserted value`() {
        val log = InMemoryLog<Long>()
        val cache = PartitionedHeadCache(log, { it.toString() }, { it })

        log.record(1)
        assertEquals(1, cache.get("1"))
    }

    @Test
    fun `should contain value inserted before cache construction`() {
        val log = InMemoryLog<Long>()
        log.record(1)
        val cache = PartitionedHeadCache(log, { it.toString() }, { it })

        assertEquals(1, cache.get("1"))
    }

    @Test
    fun `should be able to build from already partitioned log`() {
        val log = InMemoryLog<Long>()
        val partitioned = InMemoryPartitionedLog(log, Long::toString)
        val cache = PartitionedHeadCache.fromPartitionedLog(partitioned)

        log.record(7)

        assertEquals(7, cache.get("7"))
    }
}