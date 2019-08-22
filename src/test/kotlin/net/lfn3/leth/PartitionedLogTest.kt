package net.lfn3.leth

import kotlin.test.Test
import kotlin.test.assertEquals

abstract class PartitionedLogTest<T : LogWriter<Pair<Long, Long>>>(private val writerCtor: () -> T,
                                                                   private val partitionedCtor: (T) -> PartitonedLog<Long, Pair<Long, Long>>) {
    @Test
    fun `Should only see items from single partition in child logs`() {
        val writer = writerCtor()
        val partitionedLog = partitionedCtor(writer)

        val part1 = partitionedLog.get(key = 1)!!

        val seq1 = writer.record(Pair(1, 2))
        val seq2 = writer.record(Pair(2, 3))

        assertEquals(Pair<Long, Long>(1, 2), part1.get(seq1))
        assertEquals(null, part1.get(seq2))

        writer.record(Pair(1, 4))

        assertEquals(Pair<Long, Long>(1, 4), part1.head())
    }

    @Test
    fun `Size should be correct`() {
        val writer = writerCtor()
        val partitionedLog = partitionedCtor(writer)

        val part1 = partitionedLog.get(key = 1)!!
        val part2 = partitionedLog.get(key = 2)!!

        writer.record(Pair(1, 2))
        writer.record(Pair(1, 4))
        assertEquals(2, part1.size)
        writer.record(Pair(2, 3))
        assertEquals(1, part2.size)
    }
}