
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.PHONY: build
build:
	sbt clean assembly;

run:
	spark-submit --class org.sustain.clustering.SustainClustering  \
				 --supervise target/scala-2.12/sustain-clustering-assembly-0.0.4.jar;

clean:
	sbt clean
