import org.apache.pig.builtin.OutputSchema;

/**
 * Bunch of functions which are specific to parsing fetcher log information
 * in tez logs.
 */
class GroovyUDFs {

    /**
     * Normalize machine names.
     * Get the machineName without any ports or _ characters at the end
     * @param machineName
     * @return
     */
    @OutputSchema('machine:chararray')
    String getMachineName(String machineName) {
        String returnValue = machineName;
        if (machineName.contains("_")) {
            returnValue = machineName.substring(0, machineName.lastIndexOf("_"));
        }
        return returnValue;
    }

    /**
     * Read a line (specifically Shuffle related completed line and parse the data)
     * compressedSize, endTime, timeTaken, rate
     * @param line
     * @return
     */
    //TODO: Note that this function is very specific to parsing certain lines in Tez code.
    @OutputSchema('parsedCsv:chararray')
    String parseCompletedLine(String line) {
        String returnValue = "";
        if (line == null || line.trim().equals("")) {
            return returnValue;
        }
        String[] data = line.split(",");
        for(String s : data) {
            String[] kv = s.trim().split("=");
            if (kv.length == 2) {
                if (kv[0].equalsIgnoreCase("CompressedSize") || kv[0].equalsIgnoreCase("EndTime")
                        || kv[0].equalsIgnoreCase("TimeTaken") || kv[0].equalsIgnoreCase("Rate")) {
                    returnValue += kv[1] + ",";
                }
            }
        }
        if (returnValue.length() > 0) {
            returnValue = returnValue.substring(0, returnValue.length() - 1);
        }
        return returnValue;
    }

    /**
     * Read a line and parse its url
     * e.g  shuffle.HttpConnection: for url=http://mac051:13562/mapOutput?job=job_1415079080661_0429&reduce=0&map=attempt_1415079080661_0429_2_00_000000_0_10003&keepAlive=true sent hash and receievd reply 0 ms
     * would be parsed to mac051, http://mac051:13562/mapOutput?job=job_1415079080661_0429&reduce=0&map=attempt_1415079080661_0429_2_00_000000_0_10003&keepAlive=true
     *
     * @param line
     * @return
     */
    //TODO: Note that this function is very specific to parsing certain lines in Tez code.
    @OutputSchema('url:chararray')
    String processURL(String line) {
        String returnValue = "";
        if (line == null || line.trim().equals("")) {
            return returnValue;
        }
        String[] data = line.split(" ");
        for(String s : data) {
            if (s.startsWith("url=")) {
                URL url = new URL(s.replaceAll("url=", ""));
                returnValue = url.getHost() + "," + url.toString();
            }
        }
        return returnValue;
    }

    /**
     * Parse the fetcher log for shuffle timing. refer example line given below.
     * mac051:13562 freed by fetcher [Map_3] #6 in 26ms
     * @param line
     * @return
     */
    @OutputSchema('parsed:chararray')
    String getShuffleTime(String line) {
        String returnValue = "";
        if (line == null || line.trim().equals("")) {
            return returnValue;
        }
        String[] data = line.split(" ");
        if (data.length > 5) {
            returnValue += (data[0].substring(0, data[0].indexOf(":"))) + ",";
            returnValue += data[7].replaceAll("ms", "");
        }
        return returnValue;
    }
}