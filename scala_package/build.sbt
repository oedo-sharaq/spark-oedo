name := "spark-oedo-package"
version := "1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
    // Spark dependencies
    "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided",
    "org.apache.spark" %% "spark-core" % "4.0.0" % "provided",
    
    // Apache Arrow dependencies for MIRA decoder
    "org.apache.arrow" % "arrow-vector" % "14.0.0",
    "org.apache.arrow" % "arrow-memory-core" % "14.0.0",
    "org.apache.arrow" % "arrow-memory-netty" % "14.0.0",
    "org.apache.arrow" % "arrow-format" % "14.0.0",
    "org.apache.arrow" % "arrow-compression" % "14.0.0",
    
    // Parquet dependencies
    "org.apache.parquet" % "parquet-hadoop" % "1.13.1",
    "org.apache.parquet" % "parquet-arrow" % "1.13.1",
    "org.apache.parquet" % "parquet-common" % "1.13.1",
    "org.apache.parquet" % "parquet-encoding" % "1.13.1",
    "org.apache.parquet" % "parquet-column" % "1.13.1",
    
    // Hadoop dependencies (required for Parquet)
    "org.apache.hadoop" % "hadoop-client" % "3.3.6" exclude("org.slf4j", "slf4j-reload4j"),
    "org.apache.hadoop" % "hadoop-common" % "3.3.6" exclude("org.slf4j", "slf4j-reload4j"),
    
    // Logging
    "ch.qos.logback" % "logback-classic" % "1.4.11",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    
    // Testing
    "org.scalatest" %% "scalatest" % "3.2.17" % Test,
    "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test
)

// Compiler options
scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
)

// JVM options for Arrow compatibility
javaOptions in run ++= Seq(
  "-Xmx4g",
  "-XX:+UseG1GC",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

// Fork in run to avoid classpath issues
run / fork := true

// Enable parallel execution
Test / parallelExecution := true

unmanagedResourceDirectories in Compile := (unmanagedResourceDirectories in Compile).value.filterNot { dir =>
  dir.getName == "debug" // Exclude a specific folder by name
  // Or: dir == baseDirectory.value / "path/to/folderToExclude" // Exclude by full path
}
