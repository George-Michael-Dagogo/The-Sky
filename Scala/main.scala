import sys.process._

object DataExtractor {
  val path = "../The-Sky/Scala/csv_files"

  def extractDataSources(): Unit = {
    if (!new java.io.File(path).exists) {
      new java.io.File(path).mkdirs()
    }
    for (file <- new java.io.File(path).listFiles()) {
      file.delete()
    }
    val wgetCommand = s"wget -P $path -i ../The-Sky/Scala/airport.txt"
    wgetCommand.!
}
  def main(args: Array[String]): Unit = {
    extractDataSources()
  }
}

