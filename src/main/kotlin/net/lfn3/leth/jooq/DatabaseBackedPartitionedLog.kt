package net.lfn3.leth.jooq

import net.lfn3.leth.LogReader
import net.lfn3.leth.PartitonedLog
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL

//TODO: tests
class DatabaseBackedPartitionedLog<K, V, F, R : Record>(
    private val logMappings: PartitionedLogMappings<K, V, F, R>,
    private val dslProvider: () -> DSLContext
) : PartitonedLog<K, V>, LogReader<Pair<K, V>> by DatabaseBackedLogReader<Pair<K, V>, R>(
    logMappings.asReadOnlyLogMappings(DSL.trueCondition(), { true }, {
        val fromRecord = logMappings.fromRecord(it)
        Pair(logMappings.extractKey(fromRecord), fromRecord)
    }),
    dslProvider) {
    override fun get(key: K): LogReader<V>? {
        val asDbVal = logMappings.keyToDatabaseValue(key)
        val partitionLogMappings = logMappings.asReadOnlyLogMappings(logMappings.byField.eq(asDbVal)) { logMappings.extractKey(it) == key }
        return DatabaseBackedLogReader(partitionLogMappings, dslProvider)
    }

    override fun head(key: K): V? {
        val asDbVal = logMappings.keyToDatabaseValue(key)
        dslProvider().use { dsl ->
            val r = dsl.selectFrom(logMappings.table)
                .where(logMappings.byField.eq(asDbVal))
                .orderBy(logMappings.sequenceField.desc())
                .limit(1)
                .fetchOne() ?: return null

            return logMappings.fromRecord(r)
        }
    }

    //TODO: perf on this is going to be abysmal for the partitioned head cache case, since we'll hit each log once,
    // querying issuing n queries to the db where n = number of partitions.
    override fun partitions(): Map<K, LogReader<V>> {
        dslProvider().use { dsl ->
            return dsl.select(logMappings.table.fields().toList())
                .distinctOn(logMappings.byField)
                .from(logMappings.table)
                .orderBy(logMappings.byField, logMappings.sequenceField.desc())
                .fetchInto(logMappings.table)
                .map { r ->
                    val v = logMappings.fromRecord(r)
                    val k = logMappings.extractKey(v)

                    Pair(k, get(k)!!)
                }.toMap()
        }
    }
}
