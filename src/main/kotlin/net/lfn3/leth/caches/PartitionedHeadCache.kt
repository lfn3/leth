package net.lfn3.leth.caches

import net.lfn3.leth.Computed
import net.lfn3.leth.LogReader

class PartitionedHeadCache<T, K, V>(log: LogReader<T>, keyExtractor: (T) -> K, valueExtractor: (T) -> V) :
    Computed<T, Map<K, V>>(log, { newVal, state ->
        val map : HashMap<K, V> = (state ?: HashMap()) as HashMap<K, V> //Cast the map, since we know it's going to be a hashmap.
        map[keyExtractor(newVal)] = valueExtractor(newVal)
        map
    }) {
    fun get(key : K) : V? {
        return value?.get(key)
    }

    companion object {
        fun <K, V> fromPartitionedLog(log : LogReader<Pair<K, V>>) : PartitionedHeadCache<Pair<K, V>, K, V> {
            return PartitionedHeadCache(log, { it.first }, { it.second })
        }
    }
}