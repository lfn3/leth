package net.lfn3.leth.caches

import net.lfn3.leth.PartitionedHeadComputed
import net.lfn3.leth.PartitonedLog
import java.util.concurrent.ConcurrentHashMap

class PartitionedHeadCache<K, V>(log: PartitonedLog<K, V>) :
    PartitionedHeadComputed<K, V, ConcurrentHashMap<K, V>>(log, ConcurrentHashMap(), { state, key, newVal ->
        state[key] = newVal
        state
    }) {
    fun get(key : K) : V? {
        return value[key]
    }
}