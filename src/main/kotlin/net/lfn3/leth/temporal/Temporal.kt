package net.lfn3.leth.temporal

import java.time.LocalDateTime

interface Temporal {
    fun realTime() : LocalDateTime
}