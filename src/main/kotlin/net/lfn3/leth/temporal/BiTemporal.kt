package net.lfn3.leth.temporal

import java.time.LocalDateTime

interface BiTemporal : Temporal {
    fun transactionTime() : LocalDateTime
}