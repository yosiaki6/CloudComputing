package edu.cmu.cloud.backend;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Q2Tester {

	private HTable table;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("PROGRAM <hbase-address> <source-name> <table-name>");
			return;
		}
		new Q2Tester(args[0], args[1], args[2]);
	}

	private void init(String hbaseAddress, String tableName) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseAddress);
		table = new HTable(conf, tableName);
	}
	
	private String getResult(String tweetTime, String tweetUserId) throws IOException {
		String rowKey = tweetUserId + "|" + tweetTime;
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Constants.FAMILY_TWEET_ID);
		Result r = table.get(get);
		if (r.isEmpty()) {
			return null;
		}
		byte[] rawResult = r.getValue(Constants.FAMILY_TWEET_ID, null);
		String strResult = new String(rawResult);
		return strResult;
	}
	
	public Q2Tester(String hbaseAddress, String fileName, String tableName) throws Exception {
		init(hbaseAddress, tableName);
		
		int count = 0;
		Scanner inputScanner = new Scanner(new File(fileName));
		while (inputScanner.hasNext()) {
			count++;
			if (count % 10000 == 0)
				System.out.println("=== "+count+" ===");
			
			String tweetTime = inputScanner.next();
			String tweetUserId = inputScanner.next();
			String expectedResult = inputScanner.next();
			String realResult = getResult(tweetTime, tweetUserId);
			if (realResult == null)
				continue;
			if (!expectedResult.equals(realResult)) {
				System.out.println(tweetUserId + "|" +tweetTime + " => Error! Got "+realResult +" instead of "+expectedResult);
				break;
			}
		}
	}

}
