package net.lfn3.leth

interface PartitonedLog<K, V> {
    fun get(key: K) : LogReader<V>?
    fun head(key: K) : V?
}