A simple utility which uses Tez TFile loader to parse tez logs and provide it in line by line
format for distributed
processing of logs.

Currently we rely on "yarn logs -applicationId <appId> | grep blah" etc. Or we download the logs
locally and mine them.  It can be time consuming when we need to download huge volume of log files.

Build/Install:
==============
1. mvn clean package

Environment Settings in Client:
==============================
export TEZ_HOME=/grid/4/home/rajesh/tez-autobuild/dist/tez
export TEZ_CONF_DIR=$TEZ_HOME/conf
export HADOOP_CLASSPATH=$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH


Running pig with tez:
====================
1. Install pig.  Clone this project in a directory.

2. Populate param.txt.  Example param.txt is located in ./src/main/resources/pig/params.txt

3. Run 
      "PIG_CLASSPATH=$HADOOP_CLASSPATH:$PIG_CLASSPATH ./pig -x tez -m param.txt -f GetMachineDetails.pig".  This will parse the machines on which the job ran, and get distinct machine names.

4. Replace DISTINCT_MACHINES and MACHINE_MAPPING_FILE_IN_HDFS in the following commands with the values from param.txt.  This step will get the machine mapping needed for gnu plotting (basically it generates a sequence number for every machine used in the job)
      hadoop dfs -cat <DISTINCT_MACHINES>/* | awk '{print NR","$0}' > machine_mapping.csv
      hadoop dfs -put machine_mapping.csv <MACHINE_MAPPING_FILE_IN_HDFS>

5. Run the main pig script.  
     "PIG_CLASSPATH=$HADOOP_CLASSPATH:$PIG_CLASSPATH ./pig -x tez -m param.txt -f parse.pig"

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
splot '/Users/root/Downloads/end_to_end_result_for_plotting.csv' using 1:5:6 with l


