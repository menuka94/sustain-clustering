package org.alitouka.spark.dbscan.util.commandLine

import org.apache.commons.math3.ml.distance.DistanceMeasure

private [dbscan] class CommonArgsParser [C <: CommonArgs] (val args: C, programName: String)
  extends scopt.OptionParser[Unit] (programName) {

  opt[String] ("ds-master")
    .foreach { args.masterUrl = _ }
    .required ()
    .valueName ("<url>")
    .text ("Master URL")

  opt[String] ("ds-jar")
    .foreach { args.jar = _ }
    .required ()
    .valueName ("<jar>")
    .text ("Path to dbscan_prototype.jar which is visible to all nodes in your cluster")

  //TODO need to change to required later
  opt[String] ("ds-input")
    .foreach { args.inputPath = _ }
    .optional()
    .valueName("<path>")
    .text("Input path")

  //TODO need to change to required later
  opt[String] ("ds-output")
    .foreach { args.outputPath = _ }
    .optional()
    .valueName("<path>").text("Output path")

  opt[String] ("distanceMeasure").foreach {
    x => args.distanceMeasure = Class.forName(x).newInstance().asInstanceOf[DistanceMeasure]
  }

  opt[String] ("ds-debugOutput").foreach { x => args.debugOutputPath = Some(x) }

}
