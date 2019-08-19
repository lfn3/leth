package net.lfn3.leth.temporal

import java.time.LocalDateTime

data class KeyedBiTemporal(val key : Long,
                           val value : Long,
                           val realTime: LocalDateTime,
                           val transactionTime: LocalDateTime) : BiTemporal {

    override fun realTime(): LocalDateTime {
        return realTime
    }

    override fun transactionTime(): LocalDateTime {
        return transactionTime
    }
}