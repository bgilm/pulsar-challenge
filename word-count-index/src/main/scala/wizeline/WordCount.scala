package wizeline

import com.spotify.scio._
import com.spotify.scio.values.SCollection

import scala.collection.mutable.ListBuffer

import java.util.UUID.randomUUID
import scala.collection.mutable.Map
import java.util.UUID.randomUUID
import java.io.File
import java.nio.channels.Channels

import org.apache.beam.sdk.io.FileSystems

import scala.collection.JavaConverters._
import scala.io.Source
import java.util.UUID.randomUUID



/*
sbt "runMain [PACKAGE].WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=src/main/resources/datasets
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    FileSystems.setDefaultPipelineOptions(sc.options)

    // List files
    val files = FileSystems
      .`match`(args.getOrElse("input", "src/main/resources/datasets/*")) //receive input args or use default datasets
      .metadata()
      .asScala
      .map(_.resourceId().toString)

    // Read files as a collection of `(file, line)`
    val fileToContent = args.getOrElse("mode", "union") match {
      case "union" =>
        SCollection.unionAll(files.map(file => sc.textFile(file).keyBy(_ => file)))

      case "fs" =>
        sc.parallelize(files)
          .flatMap { file =>
            // Read file with the `FileSystems` API inside a worker
            val rsrc = FileSystems.matchSingleFileSpec(file).resourceId()
            val in = Channels.newInputStream(FileSystems.open(rsrc))
            Source
              .fromInputStream(in)
              .getLines()
              .map((file, _))
          }
    }

    computeWidF(args("output"), fileToContent).map{
      case (id, files) =>
        id + "," + "[" + files.split(",").toSet.mkString(",") + "]"
    }.saveAsTextFile(args("output") + "/invertedIndex")

    sc.close()
  }

  // Compute wId-FILE from an input collection of `(file, line)`
  def computeWidF(
                    path: String, fileToContent: SCollection[(String, String)]): SCollection[(java.util.UUID, String)] = {

    // Split lines into terms as (term, file)
    val fileToWords = fileToContent.flatMap {
      case (file, line) =>
        line.split("\\W+").filter(_.nonEmpty).map(w => (w.toLowerCase, file))
    }
      .map(t => (t._1, t._2))
      .reduceByKey {
      // Group all (word, fileName) pairs and concat the filePaths separated by ","
      case (n1, n2) => n1 + "," + n2
    }

    // Map (term, file) into (term:String, wid:UUID, file:String) where file is a string of concatenated file paths
    val fileWordToId = fileToWords.map {
      case (s) =>
        (s._1, randomUUID(), s._2)
    }

    // Map (word, id, files) into (word, id) while writing the dictionary (word, id)
    fileWordToId.map{
      case (word, id, files) =>
        (word, id)
    }.saveAsTextFile(path +"/dictionary")

    // return (id, files)
    return fileWordToId.map{ case (word, id, files) => (id, files)}
  }
}
