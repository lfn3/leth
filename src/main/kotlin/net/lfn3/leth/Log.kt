package net.lfn3.leth

interface Log<T> : LogReader<T>, LogWriter<T> {
    companion object {
        fun <T> from(reader: LogReader<T>, writer: LogWriter<T>) : Log<T> {
            return object : Log<T>, LogReader<T> by reader, LogWriter<T> by writer { }
        }
    }
}