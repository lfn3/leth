package net.lfn3.leth.unsafe

import net.lfn3.leth.Computed
import net.lfn3.leth.LogReader
import net.lfn3.leth.PartitonedLog

/**
 * Not thread safe
 */
class InMemoryPartitionedLog<K, V>(from: LogReader<V>, keyExtractor: (V) -> K) :
    PartitonedLog<K, V>,
    Computed<V, MutableMap<K, InMemoryLog<V>>>(from, HashMap(), { map, item ->
        val key = keyExtractor(item)
        map.compute(key) { _, l ->
            val log = l ?: InMemoryLog()

            log.record(item)

            log
        }
        map
    }), LogReader<Pair<K, V>> by LogReader.map(from, { Pair(keyExtractor(it), it) }) {
    override fun get(key: K): LogReader<V>? {
        return value[key]
    }

    override fun head(key: K): V? {
        return get(key)?.head()
    }
}