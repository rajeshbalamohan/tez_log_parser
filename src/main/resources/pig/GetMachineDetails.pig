-- Run this script to get the set of machines involved in the application log (i.e the job log you are trying to parse)
-- Running this script is a pre-requirement before running main script
-- Run the pig script "PIG_CLASSPATH=$HADOOP_CLASSPATH:$PIG_CLASSPATH ./pig -x tez -m param.txt -f GetDistinctMachines.pig"

-- Using TFileStorage itself, so that we dont need to copy the logs
set pig.splitCombination false;
set tez.grouping.min-size 5242880;
set tez.grouping.max-size 5242880;

register '/grid/4/home/rajesh/tez-autobuild/tez/tez-tools/tez-tfile-parser/target/tfile-parser-1.0-SNAPSHOT.jar';
register '/grid/4/home/rajesh/tez-autobuild/pig_tez_tfile_parser/target/udf-1.0-SNAPSHOT.jar';
register 'udf.groovy' using org.apache.pig.scripting.groovy.GroovyScriptEngine as udf;


-- Read through the logs and find out distinct machines in the cluster (machines which ran the job)
DEFINE getDistinctMachines(raw) RETURNS void {
        machines = FOREACH raw GENERATE udf.getMachineName(machine);
        distinctMachines = DISTINCT machines;
        STORE distinctMachines INTO '$DISTINCT_MACHINES' USING PigStorage(',');;
}


rmf $DISTINCT_MACHINES
rawLogs = load '$INPUT_LOGS' using org.apache.tez.tools.TFileLoader() as (machine:chararray, key:chararray, line:chararray);
raw = FOREACH rawLogs GENERATE udf.getMachineName(machine), key, line;

getDistinctMachines(raw);
exec;
