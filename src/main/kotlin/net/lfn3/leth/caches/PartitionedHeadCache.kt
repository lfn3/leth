package net.lfn3.leth.caches

import net.lfn3.leth.Computed
import net.lfn3.leth.LogReader
import java.util.concurrent.ConcurrentHashMap

class PartitionedHeadCache<T, K, V>(log: LogReader<T>, keyExtractor: (T) -> K, valueExtractor: (T) -> V) :
    Computed<T, ConcurrentHashMap<K, V>>(log, ConcurrentHashMap(), { state, newVal ->
        state[keyExtractor(newVal)] = valueExtractor(newVal)
        state
    }) {
    fun get(key : K) : V? {
        return value[key]
    }

    companion object {
        fun <K, V> fromPartitionedLog(log : LogReader<Pair<K, V>>) : PartitionedHeadCache<Pair<K, V>, K, V> {
            return PartitionedHeadCache(log, { it.first }, { it.second })
        }
    }
}