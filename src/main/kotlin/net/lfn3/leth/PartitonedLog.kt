package net.lfn3.leth

interface PartitonedLog<K, V> : LogReader<Pair<K, V>> {
    fun get(key: K) : LogReader<V>?
    fun head(key: K) : V?
    fun partitions() : Map<K, LogReader<V>>
}