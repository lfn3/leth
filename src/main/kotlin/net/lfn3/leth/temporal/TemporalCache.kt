package net.lfn3.leth.temporal

import net.lfn3.leth.PartitonedLog
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap

class TemporalCache<K, V : Temporal>(log : PartitonedLog<K, V>,
                                       private val m : MutableMap<K, NavigableMap<LocalDateTime, V>> = ConcurrentHashMap()) : Map<K, NavigableMap<LocalDateTime, V>> by m {
    init {
        log.tail {
            m.compute(it.first) { _, m ->
                val v = it.second
                val subMap = m?: ConcurrentSkipListMap();
                subMap[v.realTime()] = v
                m
            }
        }
//        TODO("This isn't quite right - I think we want to expose Map<K, V> as the interface, " +
//                "and a few more methods like:" +
//                " - getAtRealTime(K, LocalDateTime) : V" +
//                " - getHistory(K) : NavigableMap<LocalDateTime, V>()")
    }
}