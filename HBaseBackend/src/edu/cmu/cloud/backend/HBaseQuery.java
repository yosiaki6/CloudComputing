package edu.cmu.cloud.backend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseQuery {

	static final byte[] COLUMN_TWEETER_TWEETTIME = Bytes.toBytes("tweeter_tweettime"); 
	private HTable table;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) return;
		new HBaseQuery(args[0], args[1], args[2]);
	}

	private void init(String hbaseAddress) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseAddress);
		table = new HTable(conf, "tweets");
	}
	
	public HBaseQuery(String hbaseAddress, String tweeterUserId, String tweetTime) throws Exception {
		init(hbaseAddress);
		
		Scan s = new Scan();
		FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filter.addFilter(new SingleColumnValueFilter(
				COLUMN_TWEETER_TWEETTIME, null, CompareOp.EQUAL, Bytes.toBytes(tweeterUserId + "|" + tweetTime)
				));
		s.setFilter(filter);
		ResultScanner scanner = table.getScanner(s);
		try {
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				System.out.println(new String(rr.getRow()));
			}
		} finally {
			scanner.close();
		}
	}

}
