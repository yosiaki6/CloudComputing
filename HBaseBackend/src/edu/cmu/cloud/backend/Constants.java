package edu.cmu.cloud.backend;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.hadoop.hbase.util.Bytes;

public class Constants {

	// Special
	// 214445161|2014-01-23+23:06:26 => 426491141689982976;426491141706756096;
	// 2194138053|2014-01-23+23:59:44 => 426504555090935809;426504555107725312;
	// 1561497302|2014-01-23+07:01:43 => 426248362799542272;426248362807922688;
	
	// Wed Jan 22 12:21:45 +0000 2014
	static final SimpleDateFormat DATE_INPUT_FORMAT = new SimpleDateFormat(
			"E MMM d H:mm:ss Z yyyy");
	static final SimpleDateFormat DATE_OUTPUT_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd+HH:mm:ss");
	static final TimeZone TIMEZONE_UTC = TimeZone.getTimeZone("UTC");
	static final byte[] FAMILY_TWEET_ID = Bytes.toBytes("tweet_id");
	static {
		DATE_OUTPUT_FORMAT.setTimeZone(TIMEZONE_UTC);
	}
}
