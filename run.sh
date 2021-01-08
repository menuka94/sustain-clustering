#!/bin/bash

sbt clean assembly;
#spark-submit --class org.sustain.clustering.DemoDBScanDriver --supervise target/scala-2.11/sustain-clustering_2.11-0.0.4.jar;
spark-submit --class org.sustain.clustering.DemoDBScanDriver --supervise target/scala-2.11/sustain-clustering-assembly-0.0.4.jar;
