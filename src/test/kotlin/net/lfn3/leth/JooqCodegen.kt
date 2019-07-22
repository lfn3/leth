package net.lfn3.leth

import org.jooq.codegen.GenerationTool
import org.jooq.meta.jaxb.Configuration
import org.jooq.meta.jaxb.Database
import org.jooq.meta.jaxb.Generator
import org.jooq.meta.jaxb.Jdbc

object JooqCodegen {
    @JvmStatic
    fun main(args: Array<String>) {

        val driverClass = org.h2.Driver::class.java
        val jdbc = Jdbc().withDriver(driverClass.canonicalName)
            .withUrl(DEFAULT_JDBC_URL)

        val dbClass = org.jooq.meta.h2.H2Database::class.java
        val database = Database().withName(dbClass.canonicalName)
            .withInputSchema("PUBLIC")

        val target = org.jooq.meta.jaxb.Target()
            .withPackageName("net.lfn3.leth.jooq")
            .withDirectory("src/test/java/")

        val generator = Generator()
            .withDatabase(database)
            .withTarget(target)
        val configuration = Configuration()
            .withJdbc(jdbc)
            .withGenerator(generator)

        migrate().use {
            GenerationTool.generate(configuration)
        }
    }
}