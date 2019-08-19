package net.lfn3.leth

import net.lfn3.leth.caches.HeadCache
import net.lfn3.leth.unsafe.InMemoryLog
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class HeadCacheTest {
    @Test
    fun `should be empty on start`() {
        val log = InMemoryLog<Long>()
        val cache = HeadCache(log)

        assertNull(cache.get())
    }

    @Test
    fun `should contain first inserted value`() {
        val log = InMemoryLog<Long>()
        val cache = HeadCache(log)

        log.record(1)
        assertEquals(1, cache.get())
    }

    @Test
    fun `should contain value inserted before cache construction`() {
        val log = InMemoryLog<Long>()
        log.record(1)
        val cache = HeadCache(log)

        assertEquals(1, cache.get())
    }
}