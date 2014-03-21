package edu.cmu.cloud.backend;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.amazonaws.util.json.JSONObject;

public class HBaseImport {

	private HTable table;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out
					.println("PROGRAM <hbase-address> <source-name> <table-name>");
			return;
		}
		new HBaseImport(args[0], args[1], args[2]);
	}

	private void init(String hbaseAddress, String tableName) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseAddress);
		// Drop an existing table
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			if (admin.isTableEnabled(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
		}
		// Create a new table
		HColumnDescriptor hColDesc = new HColumnDescriptor(
				Constants.FAMILY_TWEET_ID);
		HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
		hTableDesc.addFamily(hColDesc);
		admin.createTable(hTableDesc);
		admin.close();
		table = new HTable(conf, tableName);
	}

	public HBaseImport(String hbaseAddress, String fileName, String tableName)
			throws Exception {
		init(hbaseAddress, tableName);
		int count = 0;
		Scanner inputScanner;
		inputScanner = new Scanner(new File(fileName));
		while (inputScanner.hasNextLine()) {
			JSONObject obj = new JSONObject(inputScanner.nextLine());

			// Get tweet ID (Column)
			String tweetId = (String) obj.get("id_str");

			// Get tweet's date and format it to an expected format
			String strInDate = (String) obj.get("created_at");
			Date inDate = Constants.DATE_INPUT_FORMAT.parse(strInDate);
			String outDate = Constants.DATE_OUTPUT_FORMAT.format(inDate);

			// Get tweeter's user ID
			JSONObject userObj = obj.getJSONObject("user");
			String userId = (String) userObj.get("id_str");

			// Check whether a record with this rowKey already exists
			String rowKey = userId + "|" + outDate;
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(Constants.FAMILY_TWEET_ID);
			Result r = table.get(get);
			String outputTweetId = "";
			if (!r.isEmpty()) {
				String current = new String(r.getValue(
						Constants.FAMILY_TWEET_ID, null));
				String[] tokens = current.split("\n");
				TreeSet<String> sorted = new TreeSet<String>();
				sorted.add(tweetId + "\n");
				for (String t : tokens) {
					sorted.add(t + "\n");
				}
				for (String s : sorted) {
					outputTweetId += s;
				}
				System.out.println("Special: " + rowKey + " => "
						+ outputTweetId);
			} else {
				outputTweetId = tweetId + "\n";
			}

			// Store tweet id(s)
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Constants.FAMILY_TWEET_ID, null,
					Bytes.toBytes(outputTweetId));
			table.put(put);

			// Print status
			count++;
			if (count % 10000 == 0) {
				System.out.println("Row " + count + ": " + rowKey + " => "
						+ tweetId);
			}
		}
		table.close();
	}

}
