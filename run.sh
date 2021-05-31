#!/bin/bash

sbt clean assembly;
source env.sh;
#spark-submit --class org.sustain.clustering.SustainClustering \
#             --supervise target/scala-2.12/sustain-clustering-assembly-0.0.4.jar;
spark-submit --class org.Neo4JConnect \
             --supervise target/scala-2.12/sustain-clustering-assembly-0.0.4.jar;
