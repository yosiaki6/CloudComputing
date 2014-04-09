import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class WarmUp {
	

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String hbaseAddress = args[0];
		Configuration hbaseConf;
		hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", hbaseAddress);
		System.out.println(hbaseConf.getInt("hbase.regionserver.handler.count", 1234) + "****");

		try {
			HTable q2table = new HTable(hbaseConf, "q2phase2");
			HTable q3table = new HTable(hbaseConf, "q3phase2");
			
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("d"));
			q2table.getScanner(scan);
			
			q2table.close();
			q3table.close();
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(1);
		} finally {
		}
		
	}

}
