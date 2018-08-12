name := "redis-scala"

organization := "com.impactua"

version := sys.env.getOrElse("TRAVIS_TAG", "2.0.0")

scalaVersion := "2.12.5"

crossScalaVersions := Seq("2.11.12", "2.12.5")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false

bintrayReleaseOnPublish := false
bintrayPackage := name.value
bintrayOrganization in bintray := Some("yarosman")
bintrayPackageLabels := Seq("scala", "redis", "netty")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)
javaOptions in Test ++= Seq("-Dio.netty.leakDetection.level=PARANOID")

val nettyVersion = "4.1.22.Final"

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.10.6.Final",
  "org.scalatest"     %% "scalatest"                    % "3.0.4" % Test,
  "com.storm-enroute" %% "scalameter"                   % "0.8.2" % Test,
  "org.slf4j"          % "slf4j-log4j12"                % "1.7.25" % Test
)
