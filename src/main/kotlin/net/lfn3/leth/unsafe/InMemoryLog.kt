package net.lfn3.leth.unsafe

import net.lfn3.leth.Log
import java.util.*

/**
 * Not thread safe
 */
class InMemoryLog<T> : Log<T> {
    private val items: MutableList<T> = ArrayList()
    private val observers : MutableList<(newEntry: T) -> Unit> = ArrayList()

    override val size: Int
        get() = items.size

    override fun isEmpty(): Boolean = items.isEmpty()

    override fun head(): T? = items.last()

    override fun get(sequence: Long): T? {
        if (items.size - 1 < sequence) {
            return null
        }
        return items[Math.toIntExact(sequence)]
    }

    override fun iterator() = ArrayList(items).iterator()

    override fun record(entry: T): Long {
        items.add(entry)
        observers.forEach { it(entry) }

        return (items.size - 1).toLong()
    }

    override fun update(getSequence: () -> Long, fn: (T) -> T) {
        val toUpdate = items.get(Math.toIntExact(getSequence()))
        val updated = fn(toUpdate)
        record(updated)
    }

    override fun tail(fn: (newEntry: T) -> Unit) {
        observers.add(fn)
    }
}