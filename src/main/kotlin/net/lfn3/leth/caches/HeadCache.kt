package net.lfn3.leth.caches

import net.lfn3.leth.HeadComputed
import net.lfn3.leth.LogReader

class HeadCache<T>(log: LogReader<T>) : HeadComputed<T, T?>(log, null, { _, v -> v }) {
    fun get() : T? {
        return value
    }
}