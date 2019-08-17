package net.lfn3.leth.bitemporal

import java.time.LocalDateTime

interface BiTemporal {
    fun realTime() : LocalDateTime
    fun transactionTime() : LocalDateTime
}