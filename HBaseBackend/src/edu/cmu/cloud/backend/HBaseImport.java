package edu.cmu.cloud.backend;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.amazonaws.util.json.JSONObject;

public class HBaseImport {

	// Wed Jan 22 12:21:45 +0000 2014
	static final SimpleDateFormat DATE_INPUT_FORMAT = new SimpleDateFormat("E MMM d H:mm:ss Z yyyy");
	static final SimpleDateFormat DATE_OUTPUT_FORMAT = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
	static final TimeZone TIMEZONE_UTC = TimeZone.getTimeZone("UTC");
	static final byte[] COLUMN_TWEETER_TWEETTIME = Bytes.toBytes("tweeter_tweettime"); 
	static {
		DATE_OUTPUT_FORMAT.setTimeZone(TIMEZONE_UTC);
	}

	private HTable table;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("PROGRAM <hbase-address> <source-name>");
			return;
		}
		new HBaseImport(args[0], args[1]);
	}

	private void init(String hbaseAddress) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseAddress);
		table = new HTable(conf, "tweets");
	}

	public HBaseImport(String hbaseAddress, String fileName) throws Exception {
		init(hbaseAddress);
		List<Put> puts = new LinkedList<Put>();

		Scanner inputScanner;		
		inputScanner = new Scanner(new File(fileName));
		while (inputScanner.hasNextLine()) {
			JSONObject obj = new JSONObject(inputScanner.nextLine());

			// Get tweet ID (RowKey)
			String tweetId = (String) obj.get("id_str");

			// Get tweet's date and format it to an expected format
			String strInDate = (String) obj.get("created_at");
			Date inDate = DATE_INPUT_FORMAT.parse(strInDate);
			String outDate = DATE_OUTPUT_FORMAT.format(inDate);

			// Get tweeter's user ID
			JSONObject userObj = obj.getJSONObject("user");
			String userId = (String) userObj.get("id_str");

			// Output
			String value = userId + "|" + outDate;
			Put put = new Put(Bytes.toBytes(tweetId));
			put.add(COLUMN_TWEETER_TWEETTIME, null, Bytes.toBytes(value));
			puts.add(put);
			System.out.println(tweetId + " => " + value);
		}
		table.put(puts);
	}

}
