package net.lfn3.leth.jooq

import net.lfn3.leth.PartitionedLogTest
import net.lfn3.leth.PartitonedLog
import net.lfn3.leth.jooq.tables.Logged
import net.lfn3.leth.jooq.tables.records.LoggedRecord
import org.junit.jupiter.api.Nested

class DatabaseBackedPartitionedLogTest {
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

    private fun makeLog(): CleanDatabaseBackedLog<Pair<Long, Long>, LoggedRecord> {
        return CleanDatabaseBackedLog(tableDescriptor)
    }

    private fun makePartitionedLog(log: CleanDatabaseBackedLog<Pair<Long, Long>, LoggedRecord>): PartitonedLog<Long, Pair<Long, Long>> {
        return DatabaseBackedPartitionedLog(
            tableDescriptor.asReadonly().asPartitioned(
                Logged.LOGGED.VALUE_ONE,
                { it },
                { it.first }), log.dslProvider
        )
    }

    @Nested
    inner class Plain : PartitionedLogTest<CleanDatabaseBackedLog<Pair<Long, Long>, LoggedRecord>>(
        this::makeLog,
        this::makePartitionedLog
    )
}