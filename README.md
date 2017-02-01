# SCARFF
SCARFF (SCAlable Real-time Frauds Finder) is a framework which enables credit card fraud detection.

# What is SCARFF
SCAlable Real-time Frauds Finder (SCARFF) is an open source platform which processes and analyses credit card streaming data in order to return reliable alerts in a nearly real-time setting. This original framework for near real-time Streaming Fraud Detection integrates Big Data tools (Kafka, Spark and Cassandra) with a machine learning approach which deals with data imbalance, non-stationarity and feedback latency.

At the core of SCARFF there is a Spark application and here we present its implementation.

A Docker image containing the tools needed to run a streaming fraud detection demo can be found in:
https://hub.docker.com/r/fabriziocarcillo/scarff/

You can follow the commands of the docker hub link or the video-tutorial:

[![ScreenShot](http://img.youtube.com/vi/GaG9J5MvfD0/0.jpg)](https://www.youtube.com/embed/GaG9J5MvfD0)



The images contains:
  * Kafka, Spark and Cassandra;
  * a compiled version of SCARFF;
  * an artificial dataset and the program to stream it.
