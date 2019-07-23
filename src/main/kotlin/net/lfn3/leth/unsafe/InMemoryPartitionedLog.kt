package net.lfn3.leth.unsafe

import net.lfn3.leth.Computed
import net.lfn3.leth.LogReader
import net.lfn3.leth.PartitonedLog
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

/**
 * Not thread safe
 */
class InMemoryPartitionedLog<K, V>(from: LogReader<V>, keyExtractor: (V) -> K) :
    PartitonedLog<K, V>,
    Computed<V, MutableMap<K, InMemoryLog<V>>>(from, { item, map ->
        val key = keyExtractor(item)
        val m = map ?: HashMap()
        m.compute(key) { _, l ->
            val log = l ?: InMemoryLog()

            log.record(item)

            log
        }
        m
    }), LogReader<Pair<K, V>> by LogReader.map(from, { Pair(keyExtractor(it), it) }) {
    override fun get(key: K): LogReader<V>? {
        return value?.get(key)
    }

    override fun head(key: K): V? {
        return get(key)?.head()
    }
}