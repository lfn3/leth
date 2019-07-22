# leth
A very work in progress implementation of a persistent log, and some utilites built on top of that abstraction.

### Building
For the first build, or if you change the [migration.sql](https://github.com/lfn3/leth/blob/master/src/test/resources/migration.sql) file,
run the main method in [JooqCodegen](https://github.com/lfn3/leth/blob/master/src/test/kotlin/net/lfn3/leth/JooqCodegen.kt) which will
produce the classes that `DatabaseBackedLogTest` depends on.
