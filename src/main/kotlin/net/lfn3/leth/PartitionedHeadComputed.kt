package net.lfn3.leth

abstract class PartitionedHeadComputed<K, V, U>(log: PartitonedLog<K, V>, init: U, op: (acc : U, K, V) -> U) {
    protected var value : U = init

    init {
        log.partitions().forEach {
            val head = it.value.head()

            if (head != null) {
                value = op(init, it.key, head)
            }
        }

        val boundOp : (Pair<K, V>) -> Unit = {
            val result = op(value, it.first, it.second)
            value = result
        }
        log.tail(boundOp)
    }
}