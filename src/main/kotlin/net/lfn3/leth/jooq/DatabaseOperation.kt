package net.lfn3.leth.jooq

import net.lfn3.leth.jooq.Sequences.NOT_SET
import org.jooq.DSLContext
import java.util.concurrent.CompletableFuture

internal abstract class DatabaseOperation<T>(
    private var seq : Long = NOT_SET,
    val result : CompletableFuture<T> = CompletableFuture()
) {
    abstract fun doOperation(dsl : DSLContext) : T

    open fun setSeq(seq : Long) {
        check(this.seq == NOT_SET) { "Seq has already been set to " + this.seq }
        this.seq = seq
    }

    fun getSeq() : Long {
        check(seq != NOT_SET) { "Seq has not been set yet" }
        return seq
    }

    fun run(dsl : DSLContext) {
        try {
            result.complete(doOperation(dsl))
        } catch (e : Exception) {
            result.completeExceptionally(e)
        }
    }
}