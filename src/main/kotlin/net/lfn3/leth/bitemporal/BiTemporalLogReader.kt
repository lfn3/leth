package net.lfn3.leth.bitemporal

import net.lfn3.leth.LogReader
import java.time.LocalDateTime

interface BiTemporalLogReader<T : BiTemporal> : LogReader<T> {
    fun atTxnTime(txnTime : LocalDateTime) : T?
    fun atRealTime(realTime : LocalDateTime) : T?
}