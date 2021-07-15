# Overview

The Akka Persistence JDBC plugin allows for using JDBC-compliant databases as backend for @extref:[Akka Persistence](akka:persistence.html) and @extref:[Akka Persistence Query](akka:persistence-query.html).

Akka Persistence JDBC writes journal and snapshot entries to a configured JDBC store. It implements the full akka-persistence-query API and is therefore very useful for implementing DDD-style application models using Akka and Scala for creating reactive applications.

Akka Persistence JDBC requires Akka $akka.version$ or later. It uses @extref:[Slick](slick:) $slick.version$ internally to access the database via JDBC, this does not require user code to make use of Slick.

## Version history

| Description | Version | Akka version |
|-------------|---------|--------------|
| New database schema, see @ref:[Migration](migration.md) | [5.0.0](https://github.com/akka/akka-persistence-jdbc/releases) | Akka 2.6.+ |
| First release within the Akka organization | [4.0.0](https://github.com/akka/akka-persistence-jdbc/releases/tag/v4.0.0) | Akka 2.6.+ |
| Requires Akka 2.5.0 | [3.5.3+](https://github.com/akka/akka-persistence-jdbc/releases/tag/v3.5.3) | Akka 2.5.23+ or 2.6.x |

See the full release history at [GitHub releases](https://github.com/akka/akka-persistence-jdbc/releases).

## Module info

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-jdbc_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-persistence-query_$scala.binary.version$
  version2=AkkaVersion
  symbol3=SlickVersion
  value3=$slick.version$
  group3=com.typesafe.slick
  artifact3=slick_$scala.binary.version$
  version3=SlickVersion
  group4=com.typesafe.slick
  artifact4=slick-hikaricp_$scala.binary.version$
  version4=SlickVersion
}

@@project-info{ projectId="core" }

## Contribution policy

Contributions via GitHub pull requests are gladly accepted from their original author. Along with any pull requests, please state that the contribution is your original work and that you license the work to the project under the project's open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project's open source license and warrant that you have the legal authority to do so.

## Code of Conduct

Contributors all agree to follow the [Lightbend Community Code of Conduct](https://www.lightbend.com/conduct).

## License

This source code is made available under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
