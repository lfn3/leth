package net.lfn3.leth.caches

import net.lfn3.leth.Computed
import net.lfn3.leth.LogReader

class HeadCache<T>(log: LogReader<T>) : Computed<T, T?>(log, null, { v, _ -> v }) {
    fun get() : T? {
        return value
    }
}