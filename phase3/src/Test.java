import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Scanner;

import org.apache.commons.lang.StringEscapeUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Test {
	private static final String NEW_LINE = "!@#NEWLINE#@!";
	
	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		Scanner scanner = new Scanner(new File("test.csv"));
		JSONParser parser = new JSONParser();
		
		String line = scanner.nextLine();
		JSONObject jsonObject;
		try {
			jsonObject = (JSONObject) parser.parse(line);
			
			String text = jsonObject.get("text").toString();
			text = StringEscapeUtils.escapeCsv(text);
			text = text.replaceAll("\n", NEW_LINE);
			
			System.out.println(text);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
	}

}
