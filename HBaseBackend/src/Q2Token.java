import java.util.Arrays;
import java.util.Scanner;


public class Q2Token {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNextLine()) {
			String[] token = scanner.nextLine().split("\t");
			System.out.println(Arrays.toString(token));
		}
	}

}
