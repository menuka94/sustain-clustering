
# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.PHONY: build
build:
	sbt assembly;

run:
	spark-submit --class org.sustain.Main  \
				 --supervise target/scala-2.12/sustain-clustering-assembly-0.0.4.jar;

run-with-java-home:
	spark-submit --class org.sustain.Main  \
				 --conf "spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk" \
				 --supervise target/scala-2.12/sustain-clustering-assembly-0.0.4.jar;

clean:
	sbt clean
