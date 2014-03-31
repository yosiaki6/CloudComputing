

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/*
	Sample data
	425970620178759680	column=tweeter_tweettime:, timestamp=1394652535677, value=635857878|2014-01-22+12:38:04
 	425970620179161088	column=tweeter_tweettime:, timestamp=1394652535677, value=2172914046|2014-01-22+12:38:04
 	425970620195557376	column=tweeter_tweettime:, timestamp=1394652535677, value=389497768|2014-01-22+12:38:04
 */

public class HBaseQuery {

	static final byte[] FAMILY_TWEET_ID = Bytes.toBytes("tweets");
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
	
	public HBaseQuery(String hbaseAddress, String tweeterUserId, String tweetTime) throws Exception {
		init(hbaseAddress);
		
		String result = getResult(tweetTime, tweeterUserId);
		if (result != null) {
			System.out.println(result);
		} else {
			System.out.println("Not found");
		}
	}

}
