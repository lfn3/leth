package net.lfn3.leth

import org.jooq.impl.DSL
import java.sql.Connection
import java.sql.DriverManager

const val DEFAULT_JDBC_URL = "jdbc:h2:mem:dblogtest"

fun migrate(jdbcUrl: String = DEFAULT_JDBC_URL) : Connection {
    val migrationScript = String::class.java.getResource("/migration.sql")
    val migrationSql = migrationScript.readText()
    val conn = DriverManager.getConnection(jdbcUrl)
    DSL.using(conn).use { dsl ->
        dsl.execute(migrationSql)
    }

    return conn
}

