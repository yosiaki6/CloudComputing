

import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;


public class Raw2CSV {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		new Raw2CSV();
	}

	public Raw2CSV() {
		TreeMap<String, TreeSet<String>> result = new TreeMap<String, TreeSet<String>>();
		
		// JSON -> TreeMap
		int rowCount = 0;
		Scanner input = new Scanner(System.in);
//		Scanner input = new Scanner(new File("/Users/kwittawat/cloud/final/100tweets.json"));
		while (input.hasNextLine()) {
			JSONObject obj;
			try {
				obj = new JSONObject(input.nextLine());
				
				// Key
				JSONObject userObj = obj.getJSONObject("user");
				String userId = (String) userObj.get("id_str");
				String strInDate = (String) obj.get("created_at");
				Date inDate;
				try {
					inDate = Constants.DATE_INPUT_FORMAT.parse(strInDate);
				} catch (ParseException e) {
					e.printStackTrace();
					continue;
				}
				String outDate = Constants.DATE_OUTPUT_FORMAT.format(inDate);
				String key = userId + "," + outDate;
				
				// Value
				String tweetId = (String) obj.get("id_str");

				// Put data
				if (!result.containsKey(key))
					result.put(key, new TreeSet<String>());
				TreeSet<String> item = result.get(key);
				item.add(tweetId);
				
				// Print status
				rowCount++;
				if (rowCount % 10000 == 0) {
					System.err.println("Row " + rowCount + ": " + key + " => "
							+ tweetId);
				}
			} catch (JSONException e) {
				e.printStackTrace();
				break;
			}
		}
		System.out.println("Row count = "+rowCount);
		
		// TreeMap -> CSV
		int outCount = 0;
		Set<Entry<String, TreeSet<String>>> entrySet = result.entrySet();
		Iterator<Entry<String, TreeSet<String>>> iter = entrySet.iterator();
		while (iter.hasNext()) {
			Entry<String, TreeSet<String>> item = iter.next();
			String key = item.getKey();
			System.out.print(key + ",");
			TreeSet<String> value = item.getValue();
			Iterator<String> iter2 = value.iterator();
			while (iter2.hasNext()) {
				System.out.print(iter2.next() + ";");
			}
			System.out.println();
			
			// Print status
			outCount++;
			if (outCount % 10000 == 0) {
				System.err.println((int)(100.0 * outCount / rowCount) + "% completed");
			}
		}
		System.err.println("Done");
	}

}
