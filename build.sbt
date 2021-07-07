import com.lightbend.paradox.apidoc.ApidocPlugin.autoImport.apidocRootPackage

lazy val `akka-persistence-jdbc` = project
  .in(file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(MimaPlugin, SitePlugin)
  .aggregate(core, docs, migrator)
  .settings(publish / skip := true)

lazy val core = project
  .in(file("core"))
  .enablePlugins(MimaPlugin)
  .disablePlugins(SitePlugin)
  .configs(IntegrationTest.extend(Test))
  .settings(Defaults.itSettings)
  .settings(
    name := "akka-persistence-jdbc",
    libraryDependencies ++= Dependencies.Libraries,
    mimaReportSignatureProblems := true,
    mimaPreviousArtifacts := Set(
      organization.value %% name.value % previousStableVersion.value.getOrElse(
        throw new Error("Unable to determine previous version for MiMa"))))

lazy val migrator = project
  .in(file("migrator"))
  .disablePlugins(SitePlugin, MimaPlugin)
  .settings(
    name := "akka-persistence-jdbc-migrator",
    libraryDependencies ++= Dependencies.Migration ++ Dependencies.Libraries,
    // TODO remove this when ready to publish it
    publish / skip := true)
  .dependsOn(core % "compile->compile;test->test")

lazy val docs = project
  .enablePlugins(ProjectAutoPlugin, AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin)
  .settings(
    name := "Akka Persistence JDBC",
    publish / skip := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-persistence-jdbc/${if (isSnapshot.value) "snapshot"
    else version.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Paradox / siteSubdirName := s"docs/akka-persistence-jdbc/${if (isSnapshot.value) "snapshot" else version.value}",
    Compile / paradoxProperties ++= Map(
      "project.url" -> "https://doc.akka.io/docs/akka-persistence-jdbc/current/",
      "canonical.base_url" -> "https://doc.akka.io/docs/akka-persistence-jdbc/current",
      "akka.version" -> Dependencies.AkkaVersion,
      "slick.version" -> Dependencies.SlickVersion,
      "extref.github.base_url" -> s"https://github.com/akka/akka-persistence-jdbc/blob/${if (isSnapshot.value) "master"
      else "v" + version.value}/%s",
      // Slick
      "extref.slick.base_url" -> s"https://scala-slick.org/doc/${Dependencies.SlickVersion}/%s",
      // Akka
      "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaBinaryVersion}/%s",
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaBinaryVersion}/",
      "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/${Dependencies.AkkaBinaryVersion}/",
      "javadoc.akka.link_style" -> "direct",
      // Java
      "javadoc.base_url" -> "https://docs.oracle.com/javase/8/docs/api/",
      // Scala
      "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/${scalaBinaryVersion.value}.x/",
      "scaladoc.akka.persistence.jdbc.base_url" -> s"/${(Preprocess / siteSubdirName).value}/"),
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io",
    apidocRootPackage := "akka")

Global / onLoad := (Global / onLoad).value.andThen { s =>
  val v = version.value
  if (dynverGitDescribeOutput.value.hasNoTags)
    throw new MessageOnlyException(
      s"Failed to derive version from git tags. Maybe run `git fetch --unshallow`? Derived version: $v")
  s
}

TaskKey[Unit]("verifyCodeFmt") := {
  scalafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Scala code found. Please run 'scalafmtAll' and commit the reformatted code")
  }
  (Compile / scalafmtSbtCheck).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted sbt code found. Please run 'scalafmtSbt' and commit the reformatted code")
  }
}

addCommandAlias("verifyCodeStyle", "headerCheck; verifyCodeFmt")
