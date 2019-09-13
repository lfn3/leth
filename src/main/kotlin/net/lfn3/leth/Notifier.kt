package net.lfn3.leth

class Notifier<T>(private val logWriter: LogWriter<T>,
                  private val onRecorded: (Long, T) -> Unit,
                  private val onBatchRecorded: (Collection<Long>, Collection<T>) -> Unit = { seqs, vals ->
                      val seqIter = seqs.iterator()
                      val valIter = vals.iterator()

                      while (seqIter.hasNext()) {
                          val seq = seqIter.next()
                          val v = valIter.next()

                          onRecorded(seq, v)
                      }
                  }) : LogWriter<T> {
    override fun record(entry: T): Long {
        val seq = logWriter.record(entry)
        onRecorded(seq, entry)
        return seq
    }

    override fun update(getSequence: () -> Long, fn: (T) -> T) : Pair<Long, T> {
        val result = logWriter.update(getSequence, fn)
        onRecorded(result.first, result.second)
        return result
    }

    override fun batchRecord(entries: Collection<T>): Collection<Long> {
        val seqs = logWriter.batchRecord(entries)
        onBatchRecorded(seqs, entries)
        return seqs
    }
}