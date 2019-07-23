package net.lfn3.leth.jooq

import net.lfn3.leth.LogReader
import net.lfn3.leth.PartitonedLog
import org.jooq.DSLContext
import org.jooq.TableRecord
import org.jooq.impl.DSL

//TODO: tests
class DatabaseBackedPartitionedLog<K, V, F, R : TableRecord<R>>(
    private val logMappings: PartitionedLogMappings<K, V, F, R>,
    private val dslProvider: () -> DSLContext
) : PartitonedLog<K, V>, LogReader<Pair<K, V>> by DatabaseBackedLogReader<Pair<K, V>, R>(
    logMappings.asReadOnlyLogMappings(DSL.trueCondition(), {
        val fromRecord = logMappings.fromRecord(it)
        Pair(logMappings.extractKey(fromRecord), fromRecord)
    }),
    dslProvider) {
    override fun get(key: K): LogReader<V>? {
        val asDbVal = logMappings.toDb(key)
        val partitionLogMappings = logMappings.asReadOnlyLogMappings(logMappings.byField.eq(asDbVal))
        return DatabaseBackedLogReader(partitionLogMappings, dslProvider)
    }

    override fun head(key: K): V? {
        val asDbVal = logMappings.toDb(key)
        dslProvider().use { dsl ->
            val r = dsl.selectFrom(logMappings.table)
                .where(logMappings.byField.eq(asDbVal))
                .orderBy(logMappings.sequenceField.desc())
                .limit(1)
                .fetchOne() ?: return null

            return logMappings.fromRecord(r)
        }
    }
}
