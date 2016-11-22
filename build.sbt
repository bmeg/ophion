organization  := "io.bmeg"
name := "leprechaun"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

// scalacOptions ++= Seq("-Ydependent-method-types")

conflictManager := ConflictManager.strict.copy(organization = "com.esotericsoftware.*")

libraryDependencies ++= Seq(
  "org.apache.tinkerpop"       %  "gremlin-core"             % "3.1.1-incubating",
  "org.scala-lang"             %  "scala-compiler"           % "2.11.8",

  "org.json4s"                 %% "json4s-native"            % "3.3.0",
  "org.json4s"                 %% "json4s-jackson"           % "3.3.0",
  "com.michaelpollmeier"       %% "gremlin-scala"            % "3.1.2-incubating.0",

  "net.jcazevedo"              %% "moultingyaml"             % "0.3.0",

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

// mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//   {
//     case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
//     case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
//     case PathList("org", "w3c", xs @ _*) => MergeStrategy.first
//     case PathList("org", "apache", "commons", "logging", xs @ _*) => MergeStrategy.first
//     case "about.html"     => MergeStrategy.discard
//     case "reference.conf" => MergeStrategy.concat
//     case "log4j.properties"     => MergeStrategy.concat
//     //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
//     case "META-INF/services/org.apache.hadoop.fs.FileSystem" => MergeStrategy.concat
//     case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//     case x => MergeStrategy.first
//   }
// }


