import java.util.function.BiConsumer

@FunctionalInterface
interface LogConsumer<T> : BiConsumer<Long, T>