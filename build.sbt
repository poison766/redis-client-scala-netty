name := "redis-scala"

organization := "com.impactua"

version := sys.env.getOrElse("TRAVIS_TAG", "2.0.0")

scalaVersion := "2.12.6"

crossScalaVersions := Seq("2.11.8", scalaVersion.value)

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

val nettyVersion = "4.1.28.Final"

libraryDependencies ++= Seq(
  "io.netty"           % "netty-handler"                  % nettyVersion,
  "io.netty"           % "netty-transport-native-epoll"   % nettyVersion classifier "linux-x86_64",
  "io.netty"           % "netty-transport-native-kqueue"  % nettyVersion classifier "osx-x86_64",
  "org.scalatest"     %% "scalatest"                      % "3.0.4" % Test,
  "com.storm-enroute" %% "scalameter"                     % "0.8.2" % Test,
  "org.slf4j"          % "slf4j-log4j12"                  % "1.7.25" % Test
)
