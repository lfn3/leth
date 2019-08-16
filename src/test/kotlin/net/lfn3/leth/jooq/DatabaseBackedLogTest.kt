package net.lfn3.leth.jooq

import net.lfn3.leth.*
import net.lfn3.leth.jooq.tables.Logged
import net.lfn3.leth.jooq.tables.records.LoggedRecord
import org.jooq.DSLContext
import org.junit.jupiter.api.Nested

class DatabaseBackedLogTest {
    private val tableDescriptor = LogWriterMappings(
        Logged.LOGGED,
        { Pair(it.valueOne, it.valueTwo) },
        Logged.LOGGED.LOG_ID,
        {
            val rec = LoggedRecord()
            rec.valueOne = it.first
            rec.valueTwo = it.second
            rec
        },
        Logged.LOGGED.ANCESTOR_ID
    )

    private fun makeLog() : Log<Pair<Long, Long>> {
        return CleanDatabaseBackedLog(tableDescriptor)
    }

    @Nested
    inner class Plain : LogTest(this::makeLog)

    @Nested
    inner class Concurrent : ConcurrentLogTest(this::makeLog)
}