import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class TestQ4 {

	private static final SimpleDateFormat QUERY_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection connect = null;
		int count = 0;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection("jdbc:mysql://localhost/cloud", "root", "root");
			PreparedStatement preparedStatement = connect.prepareStatement("select * from q4q5 where tweet_time = ? order by tweet_id");

			Scanner scanner = new Scanner(new File("q4s_answers.txt"));
			while (scanner.hasNextLine()) {
				
				String line = scanner.nextLine();
				int delimPos = line.indexOf("\t");
				String strTime = line.substring(0, delimPos);
				Date date = QUERY_DATE_FORMAT.parse(strTime);
				long unixTweetTime = date.getTime();
				String expected = line.substring(delimPos + 1);

				// parameters start with 1
				preparedStatement.setLong(1, unixTweetTime);
				ResultSet resultSet = preparedStatement.executeQuery();
				String got = "";
				while (resultSet.next()) {
					String tweetID = resultSet.getString("tweet_id");
					String text = resultSet.getString("text");
					got += tweetID + ":" + text + ";";
				}
				if (!expected.equals(got)) {
					System.err.println("Expected:\t" + expected
							+ "\nGot this:\t" + got + "\n\n");
				}

				count++;
				if (count < 161500 || count > 161700) {
					if (count % 100 == 0) {
						System.out.println("Checked "+ count);
					}
				} else {
					System.out.println("Checked "+ count);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Checked "+ count);
		} finally {
			try {
				connect.close();
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println("Checked "+ count);
			}
		}
	}

}
