package net.lfn3.leth.jooq

import net.lfn3.leth.*
import net.lfn3.leth.jooq.tables.Logged
import net.lfn3.leth.jooq.tables.records.LoggedRecord

class DatabaseBackedLogTest : LogTest({
    val tableDescriptor = LogWriterMappings(
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

    CleanDatabaseBackedLog(tableDescriptor)
})