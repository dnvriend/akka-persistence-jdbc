# Configuration

The plugin relies on @extref[Slick](slick:) to do create the SQL dialect for the database in use, therefore the following must be configured in `application.conf`

Configure `akka-persistence`:

- instruct akka persistence to use the `jdbc-journal` plugin,
- instruct akka persistence to use the `jdbc-snapshot-store` plugin,

Configure `slick`:

- The following slick profiles are supported:
  - `slick.jdbc.PostgresProfile$`
  - `slick.jdbc.MySQLProfile$`
  - `slick.jdbc.H2Profile$`
  - `slick.jdbc.OracleProfile$`
  - `slick.jdbc.SQLServerProfile$`

## Database Schema

- @extref:[Postgres Schema](github:/core/src/main/resources/schema/postgres/postgres-create-schema.sql)
- @extref:[MySQL Schema](github:/core/src/main/resources/schema/mysql/mysql-create-schema.sql)
- @extref:[H2 Schema](github:/core/src/main/resources/schema/h2/h2-create-schema.sql)
- @extref:[Oracle Schema](github:/core/src/main/resources/schema/oracle/oracle-create-schema.sql)
- @extref:[SQL Server Schema](github:/core/src/main/resources/schema/sqlserver/sqlserver-create-schema.sql)

For testing purposes the journal and snapshot tables can be created programmatically using the provided `SchemaUtils`.



Scala
:  @@snip[snip](/core/src/test/scala/akka/persistence/jdbc/ScaladslSnippets.scala) { #create }

Java
:  @@snip[snip](/core/src/test/java/akka/persistence/jdbc/JavadslSnippets.java) { #create }

A `dropIfExists` variant is also available.

**Note**: `SchemaUtils` was introduced in version 5.0.0.


## Reference Configuration

Akka Persistence JDBC provides the defaults as part of the @extref:[reference.conf](github:/core/src/main/resources/reference.conf). This file documents all the values which can be configured.

There are several possible ways to configure loading your database connections. Options will be explained below.

### One database connection pool per journal type

There is the possibility to create a separate database connection pool per journal-type (one pool for the write-journal,
one pool for the snapshot-journal, and one pool for the read-journal). This is the default and the following example
configuration shows how this is configured:

Postgres
: @@snip[Postgres](/core/src/test/resources/postgres-application.conf)

MySQL
: @@snip[MySQL](/core/src/test/resources/mysql-application.conf)

H2
: @@snip[H2](/core/src/test/resources/h2-application.conf)

Oracle
: @@snip[Oracle](/core/src/test/resources/oracle-application.conf)

SQL Server
: @@snip[SQL Server](/core/src/test/resources/sqlserver-application.conf)

### Sharing the database connection pool between the journals

In order to create only one connection pool which is shared between all journals the following configuration can be used:

Postgres
: @@snip[Postgres](/core/src/test/resources/postgres-shared-db-application.conf)

MySQL
: @@snip[MySQL](/core/src/test/resources/mysql-shared-db-application.conf)

H2
: @@snip[H2](/core/src/test/resources/h2-shared-db-application.conf)

Oracle
: @@snip[Oracle](/core/src/test/resources/oracle-shared-db-application.conf)

SQL Server
: @@snip[SQL Server](/core/src/test/resources/sqlserver-shared-db-application.conf)

### Customized loading of the db connection

It is also possible to load a custom database connection. 
In order to do so a custom implementation of @extref:[SlickDatabaseProvider](github:/src/main/scala/akka/persistence/jdbc/util/SlickExtension.scala)
needs to be created. The methods that need to be implemented supply the Slick `Database` and `Profile` to the journals.

To enable your custom `SlickDatabaseProvider`, the fully qualified class name of the `SlickDatabaseProvider`
needs to be configured in the application.conf. In addition, you might want to consider whether you want
the database to be closed automatically:

```hocon
akka.persistence.jdbc {
  database-provider-fqcn = "com.mypackage.CustomSlickDatabaseProvider"
}
jdbc-journal {
  use-shared-db = "enabled" // setting this to any non-empty string prevents the journal from closing the database on shutdown
}
jdbc-snapshot-store {
  use-shared-db = "enabled" // setting this to any non-empty string prevents the snapshot-journal from closing the database on shutdown
}
```

### DataSource lookup by JNDI name

The plugin uses `Slick` as the database access library. Slick @extref[supports jndi](slick:database.html#using-a-jndi-name) for looking up @javadoc[DataSource](javax.sql.DataSource)s.

To enable the JNDI lookup, you must add the following to your application.conf:

```hocon
jdbc-journal {
  slick {
    profile = "slick.jdbc.PostgresProfile$"
    jndiName = "java:jboss/datasources/PostgresDS"
  }
}
```

When using the `use-shared-db = slick` setting, the follow configuration can serve as an example:

```hocon
akka.persistence.jdbc {
  shared-databases {
    slick {
      profile = "slick.jdbc.PostgresProfile$"
      jndiName = "java:/jboss/datasources/bla"
    }
  }
}
```

## Explicitly shutting down the database connections

The plugin automatically shuts down the HikariCP connection pool when the ActorSystem is terminated.
This is done using @apidoc[ActorSystem.registerOnTermination](ActorSystem).
