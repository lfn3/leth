package net.lfn3.leth.temporal

import net.lfn3.leth.LogWriter
import net.lfn3.leth.PartitonedLog
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

abstract class TemporalCacheTest<T : LogWriter<KeyedBiTemporal>>(
    private val writerCtor: () -> T,
    private val partitionedCtor: (T) -> PartitonedLog<Long, KeyedBiTemporal>
) {
    private fun makeCache(writer: T): TemporalCache<Long, KeyedBiTemporal> = TemporalCache(partitionedCtor(writer))

    @Test
    fun `Should be able to get values out`() {
        val writer = writerCtor()
        val cache = makeCache(writer)

        val realTime = LocalDateTime.now()
        writer.record(KeyedBiTemporal(1, 2, realTime, LocalDateTime.now()))

        assertEquals(2, cache[1]?.get(realTime)?.value)
    }
}