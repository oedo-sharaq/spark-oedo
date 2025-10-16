name := "spark-oedo-package"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
)

// Main class for the RIDF converter application (can be overridden with --class)
mainClass in Compile := Some("RidfToParquetSimple")

unmanagedResourceDirectories in Compile := (unmanagedResourceDirectories in Compile).value.filterNot { dir =>
  dir.getName == "debug" // Exclude a specific folder by name
  // Or: dir == baseDirectory.value / "path/to/folderToExclude" // Exclude by full path
}