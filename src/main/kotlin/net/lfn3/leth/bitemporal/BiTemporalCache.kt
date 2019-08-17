package net.lfn3.leth.bitemporal

import net.lfn3.leth.LogReader
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap

class BiTemporalCache<T, K, V : BiTemporal>(log : LogReader<T>,
                                            private val getKey : (T) -> K,
                                            private val getValue : (T) -> V,
                                            private val m : MutableMap<K, NavigableMap<LocalDateTime, V>> = ConcurrentHashMap()) : Map<K, NavigableMap<LocalDateTime, V>> by m {
    init {
        TODO("This isn't quite right - I think we want to expose Map<K, V> as the interface, " +
                "and a few more methods like:" +
                " - getAtRealTime(K, LocalDateTime) : V" +
                " - getHistory(K) : NavigableMap<LocalDateTime, V>()")
        log.tail {
            m.compute(getKey(it)) { k, m ->
                val v = getValue(it)
                val subMap = m?: ConcurrentSkipListMap();
                m
            }
        }
    }
}