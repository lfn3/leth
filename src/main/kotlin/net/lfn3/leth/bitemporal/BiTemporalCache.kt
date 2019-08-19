package net.lfn3.leth.bitemporal

import net.lfn3.leth.PartitonedLog
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap

class BiTemporalCache<K, V : BiTemporal>(log : PartitonedLog<K, V>,
                                         private val m : MutableMap<K, NavigableMap<LocalDateTime, V>> = ConcurrentHashMap()) : Map<K, NavigableMap<LocalDateTime, V>> by m {
    init {
        TODO("This isn't quite right - I think we want to expose Map<K, V> as the interface, " +
                "and a few more methods like:" +
                " - getAtRealTime(K, LocalDateTime) : V" +
                " - getHistory(K) : NavigableMap<LocalDateTime, V>()")
        log.tail {
            m.compute(it.first) { k, m ->
                val v = it.second
                val subMap = m?: ConcurrentSkipListMap();
                m
            }
        }
    }
}