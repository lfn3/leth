package net.lfn3.leth.unsafe

import net.lfn3.leth.PartitionedLogTest
import net.lfn3.leth.PartitonedLog
import net.lfn3.leth.temporal.KeyedBiTemporal
import net.lfn3.leth.temporal.TemporalCacheTest
import org.junit.jupiter.api.Nested

class InMemoryPartitionedLogTest {
    private fun <T> makeLog(): InMemoryLog<T> {
        return InMemoryLog()
    }

    private fun <K, T> makePartitionedLog(log: InMemoryLog<T>, keyExtractor: (T) -> K): PartitonedLog<K, T> {
        return InMemoryPartitionedLog(log, keyExtractor)
    }

    @Nested
    inner class PartitionedLog : PartitionedLogTest<InMemoryLog<Pair<Long, Long>>>(
        this::makeLog,
        { makePartitionedLog(it, Pair<Long, Long>::first) })

    @Nested
    inner class TemporalCache :
        TemporalCacheTest<InMemoryLog<KeyedBiTemporal>>(this::makeLog, { makePartitionedLog(it, KeyedBiTemporal::key) })
}