organization  := "io.bmeg"
name := "leprechaun"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.11.8"
conflictManager := ConflictManager.strict.copy(organization = "com.esotericsoftware.*")

libraryDependencies ++= Seq(
  "org.scala-lang"             %  "scala-compiler"           % "2.11.8",
  "org.apache.tinkerpop"       %  "tinkergraph-gremlin"      % "3.1.1-incubating",

  "org.typelevel"              %% "cats"                     % "0.8.1",
  "org.http4s"                 %% "http4s-core"              % "0.14.11a",

  "org.json4s"                 %% "json4s-native"            % "3.3.0",
  "org.json4s"                 %% "json4s-jackson"           % "3.3.0",

  "org.scalactic"              %% "scalactic"                % "3.0.0",
  "org.scalatest"              %% "scalatest"                % "3.0.0" % "test"
)

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com",
  "GAEA Depends Repo" at "https://github.com/bmeg/gaea-depends/raw/master/"
)

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "content/repositories/releases")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
