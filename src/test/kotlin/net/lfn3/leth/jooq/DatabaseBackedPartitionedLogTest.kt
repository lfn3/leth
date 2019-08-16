package net.lfn3.leth.jooq

import net.lfn3.leth.jooq.tables.Logged
import net.lfn3.leth.jooq.tables.records.LoggedRecord
import kotlin.test.Test
import kotlin.test.assertEquals

class DatabaseBackedPartitionedLogTest {
    @Test
    fun shouldOnlySeeItemsFromSinglePartitionInChildLogs() {
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

        val log = CleanDatabaseBackedLog(tableDescriptor)
        val partLogMappings = tableDescriptor.asReadonly().asPartitioned(Logged.LOGGED.VALUE_ONE, { it }, { it.first })
        val partitionedLog = DatabaseBackedPartitionedLog(partLogMappings, log.dslProvider)

        val part1 = partitionedLog.get(key = 1)!!

        val seq1 = log.record(Pair(1, 2))
        val seq2 = log.record(Pair(2, 3))

        assertEquals(Pair<Long, Long>(1, 2), part1.get(seq1))
        assertEquals(null, part1.get(seq2))

        log.record(Pair(1, 4))

        assertEquals(Pair<Long, Long>(1, 4), part1.head())
    }
}