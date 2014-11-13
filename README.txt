A simple utility which uses Tez TFile loader to parse tez logs and provide it in line by line
format for distributed
processing of logs.

Currently we rely on "yarn logs -applicationId <appId> | grep blah" etc. Or we download the logs
locally and mine them.  It can be time consuming when we need to download huge volume of log files.

Build/Install:
==============
1. mvn clean package


Running pig with tez:
====================
1. Install pig
2. Populate param.txt
3. "PIG_CLASSPATH=$HADOOP_CLASSPATH:$PIG_CLASSPATH ./pig -x tez -m param.txt -f parse.pig"

Plot graph:
==========
1. Download "end_to_end_result_for_plotting.csv" from HDFS
2. Use gnuplot for plotting the surface plot

e.g
set xlabel 'source-machine'
set ylabel 'destination-machine'
set zlabel 'Time taken'
set pm3d
set palette
set hidden3d
set dgrid3d 50,50 qnorm 2
set datafile separator ','
set view 55,32,1,1
set dgrid3d 500,500 qnorm 2
set contour base
splot '/Users/rbalamohan/Downloads/end_to_end_result_for_plotting.csv' using 1:5:6 with l


