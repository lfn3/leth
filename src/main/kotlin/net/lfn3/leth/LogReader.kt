package net.lfn3.leth

interface LogReader<T> : Collection<T> {
    fun head() : T?
    fun get(sequence: Long) : T?
    fun tail(fn : (newEntry: T) -> Unit)
}