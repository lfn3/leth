package net.lfn3.leth.unsafe

import net.lfn3.leth.Log
import java.lang.IllegalArgumentException
import java.util.*

/**
 * Not thread safe
 */
class InMemoryLog<T> : Log<T> {
    private val items: MutableList<T> = ArrayList()
    private val observers : MutableList<(newEntry: T) -> Unit> = ArrayList()

    override val size: Long
        get() = items.size.toLong()

    override fun isEmpty(): Boolean = items.isEmpty()

    override fun head(): T? = if (isEmpty()) { null } else { items.last() }

    override fun headWithSeq(): Pair<Long, T>? {
        val head = head() ?: return null
        return Pair((items.size - 1).toLong(), head)
    }

    override fun get(sequence: Long): T? {
        if (items.size - 1 < sequence) {
            return null
        }
        return items[Math.toIntExact(sequence)]
    }

    override fun iterator() = ArrayList(items).iterator()

    override fun tail(fn: (newEntry: T) -> Unit) {
        observers.add(fn)
    }

    override fun tail(start: Long, fn: (T) -> Unit) {
        items.subList(start.toInt(), items.size).forEach(fn)
        observers.add(fn)
    }

    override fun record(entry: T): Long {
        items.add(entry)
        observers.forEach { it(entry) }

        return (items.size - 1).toLong()
    }

    override fun update(getSequence: () -> Long, fn: (T) -> T) {
        val toUpdate = items[Math.toIntExact(getSequence())]
        val updated = fn(toUpdate)

        if (updated == toUpdate) {
            return
        }

        record(updated)
    }
}