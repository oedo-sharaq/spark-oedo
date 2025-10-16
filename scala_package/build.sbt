name := "spark-oedo-package"
version := "1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided",
    "org.apache.spark" %% "spark-core" % "4.0.0" % "provided"
)

// Main class for the RIDF converter application (can be overridden with --class)
mainClass in Compile := Some("RidfToParquetSimple")

unmanagedResourceDirectories in Compile := (unmanagedResourceDirectories in Compile).value.filterNot { dir =>
  dir.getName == "debug" // Exclude a specific folder by name
  // Or: dir == baseDirectory.value / "path/to/folderToExclude" // Exclude by full path
}
