-- Ensure that you have read README.txt.  Ensure you have copied the machine_mapping.csv to HDFS.
-- Run the pig script "PIG_CLASSPATH=$HADOOP_CLASSPATH:$PIG_CLASSPATH ./pig -x tez -m param.txt -f parse.pig"
-- Download "END_TO_END_TIMINGS_FOR_PLOTTING" from HDFS
-- Plot the graph

-- Using TFileStorage itself, so that we dont need to copy the logs
set pig.splitCombination false;
set tez.grouping.min-size 5242880;
set tez.grouping.max-size 5242880;

register '/grid/4/home/rajesh/tez-autobuild/tez/tez-tools/tez-tfile-parser/target/tfile-parser-1.0-SNAPSHOT.jar';
register '/grid/4/home/rajesh/tez-autobuild/pig_tez_tfile_parser/target/udf-1.0-SNAPSHOT.jar';
register 'udf.groovy' using org.apache.pig.scripting.groovy.GroovyScriptEngine as udf;

define quantile datafu.pig.stats.Quantile('0.0','0.10','0.20','0.30', '0.40', '0.50','0.60', '0.70','0.80', '0.90','1.0');

-- Analyze fetcher logs
DEFINE parseFetcherData(raw) RETURNS void {
		f = FILTER raw BY (line MATCHES '.*Completed.*') AND (line MATCHES '.*CompressedSize.*' OR
			line MATCHES '.*EndTime.*' OR line MATCHES '.*TimeTaken.*' OR line MATCHES '.*Rate.*');
		-- completed: {machine: chararray,data::time: chararray,data::logLevel: chararray,data::threadName: chararray,data::packageName: chararray,data::message: chararray}
		completed = FOREACH f GENERATE machine, FLATTEN(org.pig.udf.logs.Log4jParser(line));
		completedWithTime  = FOREACH completed GENERATE machine, org.pig.udf.logs.ParseTime(data::time) as time, data::threadName as threadName, data::message as message;
		-- machine, time, vertex, threadID, compressedSize, endTime, timeTaken, rate
    attemptInfo = FOREACH completedWithTime GENERATE machine, time, REGEX_EXTRACT(threadName, '\\[(.*)\\](.*)',1) as vertexName, TRIM(REGEX_EXTRACT(threadName, '\\[(.*)\\](.*)', 2)) as threadName, udf.parseCompletedLine(message);
		STORE attemptInfo INTO '$ATTEMPT_INFO' USING PigStorage(',');

		f = FILTER raw BY line MATCHES '.*for url.*';
		completed = FOREACH f GENERATE machine, FLATTEN(org.pig.udf.logs.Log4jParser(line));
    completedWithTime  = FOREACH completed GENERATE machine, org.pig.udf.logs.ParseTime(data::time) as time, data::threadName as threadName, data::message as message;
    -- machine, time, vertex, threadID, destination, query
    httpLinesFiltered = FOREACH completedWithTime GENERATE machine, time, REGEX_EXTRACT(threadName, '\\[(.*)\\](.*)',1) as vertexName, TRIM(REGEX_EXTRACT(threadName, '\\[(.*)\\](.*)', 2)) as threadName,udf.processURL(message);
    STORE httpLinesFiltered INTO '$SRC_TO_ATTEMPT_INFO' USING PigStorage(',');
}


-- Generate end to end shuffle time (i.e connect time till the last attempt fetch per source->destination)
-- 2014-09-25 18:02:07,830 INFO [fetcher [Map_5] #25 cn041-10.l42scl.hortonworks.com/172.19.128.41] org.apache.tez.runtime.library.common.shuffle.impl.ShuffleScheduler: cn041-10.l42scl:13562 freed by fetcher
DEFINE generateEndToEndShuffleTime(raw) RETURNS void {
	freed = FILTER raw BY line MATCHES '.*freed.*';
  completed = FOREACH freed GENERATE machine, FLATTEN(org.pig.udf.logs.Log4jParser(line));
  c  = FOREACH completed GENERATE machine, org.pig.udf.logs.ParseTime(data::time) as time, data::threadName as threadName, data::message as message;
  d = FOREACH c GENERATE machine, time, REGEX_EXTRACT(threadName, '\\[(.*)\\](.*)',1) as vertexName, TRIM(REGEX_EXTRACT(threadName, '\\[(.*)\\](.*)', 2)) as threadName,udf.getShuffleTime(message);
	-- src, startTime, vertexName, threadId, destination, time(ms)
	STORE d INTO '$END_TO_END_TIMINGS' USING PigStorage(',');
}

-- Get the time taken from source to destination
DEFINE getShuffleTimeForPlotting() RETURNS void {
	m = load '$MACHINE_MAPPING_FILE_IN_HDFS' using PigStorage(',') as (id:int, machine:chararray);
  data = LOAD '$END_TO_END_TIMINGS' USING PigStorage(',') AS (src:chararray, stime:chararray,
  		vertexName:chararray, threadId:chararray, destination:chararray, time:long);
  srcJoin = JOIN m by machine, data by src;
  srcReplaced = FOREACH srcJoin GENERATE m::id, data::stime, data::vertexName, data::threadId, data::destination,data::time;
  destJoin = JOIN m by machine, srcReplaced by data::destination;
  destReplaced = FOREACH destJoin GENERATE srcReplaced::m::id,srcReplaced::data::stime,srcReplaced::data::vertexName,srcReplaced::data::threadId,m::id,srcReplaced::data::time;
  dis = DISTINCT destReplaced;
  --order by worst timing first (even if we do sample, let bad machines crop up first)
  ordered = ORDER dis BY srcReplaced::data::time DESC PARALLEL 20;
  --src_id, stime,vertex, threadid, dest_id, time
  STORE ordered INTO '$END_TO_END_TIMINGS_FOR_PLOTTING' USING PigStorage(',');
}


-- At a per thread and per attempt level, generate the source to destination mapping
DEFINE generateSourceToDestinationMappingPerAttempt() RETURNS void {
-- machine, time, vertex, threadID, compressedSize, endTime, timeTaken, rate
	rate1 = LOAD '$ATTEMPT_INFO' using PigStorage(',') AS
	 (src:chararray, millis:long, vertex:chararray,
		threadId:chararray, compressedSize:chararray, endTime:long, timeTaken:long,
		transferRate:chararray);
	rate = FOREACH rate1 GENERATE millis, vertex, threadId, src, timeTaken, transferRate, compressedSize;
	-- machine, time, vertex, threadID, destination, query
	srcToAttempt1 = LOAD '$SRC_TO_ATTEMPT_INFO' using PigStorage(',') as
	 (src:chararray, millis:long, vertex:chararray, threadId:chararray, dest:chararray,query:chararray);
	srcToAttempt = FOREACH srcToAttempt1 GENERATE millis, vertex, threadId, src, dest, query;
	j = JOIN rate BY (vertex, threadId, src),  srcToAttempt BY (vertex, threadId,src) PARALLEL 20;
	result = FOREACH j GENERATE rate::millis, rate::vertex, rate::threadId, rate::src, srcToAttempt::dest, rate::timeTaken, rate::transferRate, rate::compressedSize;
	STORE result INTO '$ATTEMPT_TIMINGS' USING PigStorage(',');
}


rmf $DISTINCT_MACHINES
rmf $ATTEMPT_INFO
rmf $SRC_TO_ATTEMPT_INFO
rmf $END_TO_END_TIMINGS
rmf $END_TO_END_TIMINGS_FOR_PLOTTING
rawLogs = load '$INPUT_LOGS' using org.apache.tez.tools.TFileLoader() as (machine:chararray, key:chararray, line:chararray);
raw = FOREACH rawLogs GENERATE udf.getMachineName(machine), key, line;

parseFetcherData(raw);
exec;

generateEndToEndShuffleTime(raw);
exec;

getShuffleTimeForPlotting();
exec;

-- TODO: Don't use this yet.  Need to fix the bugs
-- generateSourceToDestinationMappingPerAttempt();
--exec;
