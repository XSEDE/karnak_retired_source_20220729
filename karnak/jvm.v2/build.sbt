scalacOptions += "-target:jvm-1.7"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

name := "karnak"

version := "0.1"

publishMavenStyle := true

crossPaths := false
//autoScalaLibrary := false

libraryDependencies += "commons-daemon" % "commons-daemon" % "1.0.15"

libraryDependencies += "com.weiglewilczek.slf4s" % "slf4s_2.9.1" % "1.0.7"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.12"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"

// fork causes problems and javaOptions only works with fork
//fork := true
//javaOptions += "-Dlog4j.configuration=file:///home/karnak/karnak/etc/log4j.properties"

//libraryDependencies += "org.apache.logging.log4j" % "log4j" % "2.2"
libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.35"

///**** for the client library ****/

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.4.1"

///**** for the web service ****/

libraryDependencies += "org.glassfish.grizzly" % "grizzly-framework" % "2.3.19"

libraryDependencies += "org.glassfish.jersey" % "jersey-bom" % "2.17"

libraryDependencies += "org.glassfish.jersey.containers" % "jersey-container-grizzly2-http" % "2.17"

//Jungha added for test
libraryDependencies += "junit" % "junit" % "4.8" % "test"

libraryDependencies += "org.apache.commons" % "commons-collections4" % "4.1"
