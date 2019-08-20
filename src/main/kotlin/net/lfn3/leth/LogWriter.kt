package net.lfn3.leth

interface LogWriter<T> {
    fun record(entry: T) : Long
    /**
     * This is a no-op if the returned value is null, or if the updated value == the existing one.
     */
    fun update(getSequence: () -> Long, fn: (T) -> T)

    fun batchRecord(entries: Collection<T>) {
        entries.forEach { record(it) }
    }
}