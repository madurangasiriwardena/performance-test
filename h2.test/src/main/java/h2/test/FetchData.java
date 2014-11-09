package h2.test;
import h2.test.DataSourceException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.axiom.om.OMException;

public class FetchData {
	Connection conn = null;
	ResultSet rs = null;

	long start, end;
	int iteration = 0;

	public static void main(String[] args) throws OMException, IOException,
			SQLException {
		
		String driverName = "org.h2.Driver";
		String url = "jdbc:h2:tcp://localhost/test";
//		String url = "jdbc:h2:tcp://192.248.15.239:9092/test";
		String uname="maduranga";
		String pwd="maduranga";
		
		FetchData fd = new FetchData();
//		fd.testConnection(driver, url, uname, pwd);
		fd.driverSimulator(driverName,url,uname,pwd);
	}
	
	public void driverSimulator(String driverName,String url,String uname,String pwd) throws OMException, IOException, SQLException {
		
		try {
			boolean valiedConnection = testConnection(driverName, url,
					uname, pwd);
			if (valiedConnection) {
				System.out.println("Connection is Healthy");
				for(int j=0; j<6;j++){
					writeToFile("\n");
					writeToFile(iteration+",");
					query1();
					writeToFile(",");
					query2();
					writeToFile(",");
					query3();
					writeToFile(",");
					query4();
					writeToFile(",");
					query5();
					writeToFile(",");
					query6();
					writeToFile(",");
					query7();
					writeToFile(",");
					query8();
					writeToFile(",");
					query9();
					writeToFile(",");
					query10();
					writeToFile(",");
					query11();
					writeToFile(",");
					query12();
					writeToFile(",");
					query13();
					writeToFile(",");
					query14();
					writeToFile(",");
					query15();
					writeToFile(",");
					query16();
					writeToFile(",");
					query17();
					
				}
			}
		} catch (DataSourceException e) {
			System.out.println(e.getMessage());
		}

	}

	public void driverSimulator(Connection con, int i) throws OMException, IOException, SQLException {
		conn = con;
		iteration = i;
		
		addIndex();
//		try {
//			boolean valiedConnection = testConnection(driverName, url,
//					uname, pwd);
//			if (valiedConnection) {
				System.out.println("Connection is Healthy");
				for(int j=0; j<6;j++){
					writeToFile("\n");
					writeToFile(iteration+",");
					query1();
					writeToFile(",");
					query2();
					writeToFile(",");
					query3();
					writeToFile(",");
					query4();
					writeToFile(",");
					query5();
					writeToFile(",");
					query6();
					writeToFile(",");
					query7();
					writeToFile(",");
					query8();
					writeToFile(",");
					query9();
					writeToFile(",");
					query10();
					writeToFile(",");
					query11();
					writeToFile(",");
					query12();
					writeToFile(",");
					query13();
					writeToFile(",");
					query14();
					writeToFile(",");
					query15();
					writeToFile(",");
					query16();
					writeToFile(",");
					query17();
				}
//			}
//		} catch (DataSourceException e) {
//			System.out.println(e.getMessage());
//		}
				dropIndex();

	}
	
	public void writeToFile(String s){
		try {
			PrintWriter out = new PrintWriter(
					new BufferedWriter(new FileWriter(
							"/home/lahiru/h2/Query.txt", true)));
			out.append(s);
			out.close();
			System.out.println(s);
		} catch (IOException e) {
			
		}
	}

	public void test() throws SQLException {
		String s = "Select SP_INSERT_UPDATE_WORD(?) AS WORD_INDEX";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, "මාංස");
		rs = stmt6.executeQuery();
		
//		System.out.println(a);
		long word;
		if (rs.next()) {
			System.out.println("aaaaa");
			word = rs.getLong("WORD_INDEX");
			System.out.println(word);
			System.out.println("bbbbb");			
		}
	}
	
	public void dropIndex(){
		String[] indexNames = {
				"WORD_FREQUENCY_IDX",
				"YEAR_IDX",
				"CATEGORY_IDX",
				"BIGRAM_IDX",
				"BIGRAM_WORD1_IDX",
				"BIGRAM_WORD2_IDX",
				"TRIGRAM_IDX",
				"TRIGRAM_WORD1_IDX",
				"TRIGRAM_WORD2_IDX",
				"TRIGRAM_WORD3_IDX",
				"POSITION_IDX"};
		String s = "DROP INDEX IF EXISTS ";
		PreparedStatement stmt;
		for(int i=0; i<indexNames.length; i++){
			try {
				stmt = conn.prepareStatement(s+indexNames[i]);
				stmt.execute();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public void addIndex(){
		String[] index = {
				"CREATE INDEX WORD_FREQUENCY_IDX ON WORD(FREQUENCY )",
				"CREATE INDEX YEAR_IDX ON POST(YEAR )",
				"CREATE INDEX CATEGORY_IDX ON POST(CATEGORY )",
				"CREATE INDEX BIGRAM_IDX ON BIGRAM(CONTENT )",
				"CREATE INDEX BIGRAM_WORD1_IDX ON BIGRAM(WORD1_INDEX )",
				"CREATE INDEX BIGRAM_WORD2_IDX ON BIGRAM(WORD2_INDEX )",
				"CREATE INDEX TRIGRAM_IDX ON TRIGRAM(CONTENT )",
				"CREATE INDEX TRIGRAM_WORD1_IDX ON TRIGRAM(WORD1_INDEX )",
				"CREATE INDEX TRIGRAM_WORD2_IDX ON TRIGRAM(WORD2_INDEX )",
				"CREATE INDEX TRIGRAM_WORD3_IDX ON TRIGRAM(WORD3_INDEX )",
				"CREATE INDEX POSITION_IDX ON SENTENCE_WORD (POSITION )"};
		
		PreparedStatement stmt;
		for(int i=0; i<index.length; i++){
			try {
				stmt = conn.prepareStatement(index[i]);
				stmt.execute();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	public void query1() throws SQLException {
		String word = "මහින්ද";
		int frequency = 0;
		String s = "SELECT FREQUENCY FROM WORD WHERE CONTENT = ?";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
			if (rs.next()) {
				frequency = rs.getInt("FREQUENCY");
	
			}
			
			System.out.println("Frequency of word " + word + " is " + frequency);

		writeToFile(((end - start)/1000000)+"");
		

		rs.close();
		rs = null;		
		
	}

	public void query2() throws SQLException {
		String word = "මහින්ද";
		int year = 2010;
		int frequency = 0;
		
		String s = "SELECT COUNT(*) AS COUNT FROM (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) AS W LEFT JOIN SENTENCE_WORD ON W.WORD_INDEX=SENTENCE_WORD.WORD_INDEX LEFT JOIN SENTENCE ON SENTENCE_WORD.SENTENCE_INDEX = SENTENCE .SENTENCE_INDEX LEFT JOIN POST ON SENTENCE .POST_INDEX =POST .POST_INDEX  WHERE YEAR= ?";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setInt(2, year);
		stmt6.setString(1, word);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		if (rs.next()) {
			frequency = rs.getInt("COUNT");

		}
		
		System.out.println("Frequency of word " + word + " in " + year + " is "
				+ frequency);

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}

	public void query3() throws SQLException {
		String word = "මහින්ද";
		int year = 2011;
		int frequency = 0;
		
		String s = "SELECT COUNT(*) AS COUNT FROM (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) AS W LEFT JOIN SENTENCE_WORD ON W.WORD_INDEX=SENTENCE_WORD.WORD_INDEX LEFT JOIN SENTENCE ON SENTENCE_WORD.SENTENCE_INDEX = SENTENCE .SENTENCE_INDEX LEFT JOIN POST ON SENTENCE .POST_INDEX =POST .POST_INDEX  WHERE YEAR= ?";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setInt(2, year);
		stmt6.setString(1, word);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		if (rs.next()) {
			frequency = rs.getInt("COUNT");

		}
		
		System.out.println("Frequency of word " + word + " in " + year + " is "
				+ frequency);

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query4() throws SQLException {
		String word = "මහින්ද";
		int year = 2012;
		int frequency = 0;
		
		String s = "SELECT COUNT(*) AS COUNT FROM (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) AS W LEFT JOIN SENTENCE_WORD ON W.WORD_INDEX=SENTENCE_WORD.WORD_INDEX LEFT JOIN SENTENCE ON SENTENCE_WORD.SENTENCE_INDEX = SENTENCE .SENTENCE_INDEX LEFT JOIN POST ON SENTENCE .POST_INDEX =POST .POST_INDEX  WHERE YEAR= ?";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setInt(2, year);
		stmt6.setString(1, word);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		if (rs.next()) {
			frequency = rs.getInt("COUNT");

		}
		
		System.out.println("Frequency of word " + word + " in " + year + " is "
				+ frequency);

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}

	public void query5() throws SQLException {
		int years = 2010;
		
		String s = "SELECT WORD.CONTENT AS WORD, COUNT(*) AS COUNT FROM (SELECT POST_INDEX FROM POST WHERE YEAR=?) AS P LEFT JOIN SENTENCE ON P .POST_INDEX=SENTENCE .POST_INDEX LEFT JOIN SENTENCE_WORD ON SENTENCE .SENTENCE_INDEX=SENTENCE_WORD.SENTENCE_INDEX LEFT JOIN WORD ON SENTENCE_WORD.WORD_INDEX=WORD.WORD_INDEX GROUP BY WORD.WORD_INDEX  ORDER BY COUNT DESC LIMIT 10";
		PreparedStatement stmt6 = conn.prepareStatement(s);
			stmt6.setInt(1, years);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Frequent words in a time period");
		
		while (rs.next()) {
			System.out.println(rs.getString("WORD"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query6() throws SQLException {
		String word = "මහින්ද";

		String s = "SELECT SENTENCE.CONTENT FROM (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) AS W LEFT JOIN SENTENCE_WORD ON W.WORD_INDEX=SENTENCE_WORD.WORD_INDEX LEFT JOIN SENTENCE ON SENTENCE_WORD.SENTENCE_INDEX = SENTENCE .SENTENCE_INDEX LEFT JOIN POST ON SENTENCE .POST_INDEX =POST .POST_INDEX ORDER BY YEAR DESC, MONTH DESC, DAY DESC LIMIT 50";

		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Latest sentences with word " + word);
		while (rs.next()) {
			System.out.println(rs.getString("CONTENT"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query7() throws SQLException {
		String word = "මහින්ද";
		int years = 2010;
		
		String s = "SELECT SENTENCE.CONTENT FROM (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) AS W LEFT JOIN SENTENCE_WORD ON W.WORD_INDEX=SENTENCE_WORD.WORD_INDEX LEFT JOIN SENTENCE ON SENTENCE_WORD.SENTENCE_INDEX = SENTENCE .SENTENCE_INDEX LEFT JOIN POST ON SENTENCE .POST_INDEX =POST .POST_INDEX WHERE YEAR= ? ORDER BY YEAR DESC, MONTH DESC, DAY DESC LIMIT 50";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		stmt6.setInt(2, years);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Latest sentences with word " + word + " in time period");
		while (rs.next()) {
			System.out.println(rs.getString("CONTENT"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query8() throws SQLException {
		String word = "මහින්ද";
		int years = 2011;
		
		String s = "SELECT SENTENCE.CONTENT FROM (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) AS W LEFT JOIN SENTENCE_WORD ON W.WORD_INDEX=SENTENCE_WORD.WORD_INDEX LEFT JOIN SENTENCE ON SENTENCE_WORD.SENTENCE_INDEX = SENTENCE .SENTENCE_INDEX LEFT JOIN POST ON SENTENCE .POST_INDEX =POST .POST_INDEX WHERE YEAR= ? ORDER BY YEAR DESC, MONTH DESC, DAY DESC LIMIT 50";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		stmt6.setInt(2, years);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Latest sentences with word " + word + " in time period");
		while (rs.next()) {
			System.out.println(rs.getString("CONTENT"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query9() throws SQLException {
		String word = "මහින්ද";
		int years = 2012;
		
		String s = "SELECT SENTENCE.CONTENT FROM (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) AS W LEFT JOIN SENTENCE_WORD ON W.WORD_INDEX=SENTENCE_WORD.WORD_INDEX LEFT JOIN SENTENCE ON SENTENCE_WORD.SENTENCE_INDEX = SENTENCE .SENTENCE_INDEX LEFT JOIN POST ON SENTENCE .POST_INDEX =POST .POST_INDEX WHERE YEAR= ? ORDER BY YEAR DESC, MONTH DESC, DAY DESC LIMIT 50";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		stmt6.setInt(2, years);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Latest sentences with word " + word + " in time period");
		while (rs.next()) {
			System.out.println(rs.getString("CONTENT"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query10() throws SQLException {
		int position = 1; //    >0
		
		String s = "SELECT WORD.WORD_INDEX, WORD.CONTENT, COUNT(*) AS COUNT FROM (SELECT WORD_INDEX FROM SENTENCE_WORD WHERE POSITION =?) AS S LEFT JOIN WORD ON S.WORD_INDEX =WORD .WORD_INDEX GROUP BY WORD.WORD_INDEX ORDER BY COUNT DESC LIMIT 50";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setLong(1, position);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Most frequent words in the position " + position + " of a sentence");
		while (rs.next()) {
			System.out.println(rs.getString("CONTENT"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query11() throws SQLException {
		//most 10 frequent words after a word
		String word = "මහින්ද";
		
		String s = "SELECT W2.CONTENT, COUNT(*) AS COUNT FROM WORD AS W1, WORD AS W2, SENTENCE_WORD AS SW1, SENTENCE_WORD AS SW2 WHERE SW1.SENTENCE_INDEX =SW2.SENTENCE_INDEX AND W1.WORD_INDEX =SW1.WORD_INDEX AND W2.WORD_INDEX =SW2.WORD_INDEX AND W1.CONTENT =? AND SW1.POSITION =SW2.POSITION-1 GROUP BY W2.CONTENT ORDER BY COUNT DESC LIMIT 10";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Most frequent words after " + word);
		while (rs.next()) {
			System.out.println(rs.getString("CONTENT"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query12() throws SQLException {
		String word = "මහින්ද රාජපක්ෂ";
		int year = 2010;
		int frequency = 0;
		
		String s = "SELECT COUNT(*) AS COUNT FROM (SELECT * FROM BIGRAM  WHERE CONTENT = ?) AS B LEFT JOIN SENTENCE_BIGRAM ON B.BIGRAM_INDEX=SENTENCE_BIGRAM.BIGRAM_INDEX LEFT JOIN SENTENCE ON SENTENCE_BIGRAM.SENTENCE_INDEX = SENTENCE.SENTENCE_INDEX LEFT JOIN POST ON SENTENCE.POST_INDEX = POST.POST_INDEX WHERE YEAR =?";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		stmt6.setInt(2, year);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();

		if (rs.next()) {
			frequency = rs.getInt("COUNT");

		}
		
		System.out.println("Frequency of word " + word + " in " + year + " is "
				+ frequency);

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query13() throws SQLException {
		String word = "ජනාධිපති මහින්ද රාජපක්ෂ";
		int year = 2010;
		int frequency = 0;
		
		String s = "SELECT COUNT(*) AS COUNT FROM (SELECT * FROM TRIGRAM  WHERE CONTENT = ?) AS B LEFT JOIN SENTENCE_TRIGRAM ON B.TRIGRAM_INDEX=SENTENCE_TRIGRAM.TRIGRAM_INDEX LEFT JOIN SENTENCE ON SENTENCE_TRIGRAM.SENTENCE_INDEX = SENTENCE.SENTENCE_INDEX LEFT JOIN POST ON SENTENCE.POST_INDEX = POST.POST_INDEX WHERE YEAR =?";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		stmt6.setInt(2, year);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();

		if (rs.next()) {
			frequency = rs.getInt("COUNT");

		}
		
		System.out.println("Frequency of word " + word + " in " + year + " is "
				+ frequency);

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query14() throws SQLException {		
		String s = "SELECT BIGRAM .CONTENT AS BIGRAM , FREQUENCY  FROM BIGRAM  ORDER BY FREQUENCY DESC LIMIT 10";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Most frequent bigrams");
		while (rs.next()) {
			System.out.println(rs.getString("BIGRAM"));
		}
		
		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query15() throws SQLException {		
		String s = "SELECT TRIGRAM .CONTENT AS TRIGRAM, FREQUENCY FROM TRIGRAM ORDER BY FREQUENCY DESC LIMIT 10";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Most frequent trigrams");
		while (rs.next()) {
			System.out.println(rs.getString("TRIGRAM"));
		}
		
		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}

	public void query16() throws SQLException {
		//most 10 frequent words after a word
		String word = "මහින්ද";
		
		String s = "SELECT CONTENT  FROM (SELECT WORD2_INDEX FROM BIGRAM WHERE WORD1_INDEX = (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) ORDER BY FREQUENCY DESC LIMIT 10) LEFT JOIN WORD ON WORD_INDEX =WORD2_INDEX";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Most frequent words after " + word);
		while (rs.next()) {
			System.out.println(rs.getString("CONTENT"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}
	
	public void query17() throws SQLException {
		//most 10 frequent words after a word
		String word1 = "මහින්ද";
		String word2 = "රාජපක්ෂ";
		
		String s = "SELECT CONTENT  FROM (SELECT WORD3_INDEX FROM TRIGRAM WHERE WORD1_INDEX = (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) AND WORD2_INDEX = (SELECT WORD_INDEX FROM WORD WHERE WORD.CONTENT = ?) ORDER BY FREQUENCY DESC LIMIT 10) LEFT JOIN WORD ON WORD_INDEX =WORD3_INDEX";
		PreparedStatement stmt6 = conn.prepareStatement(s);
		stmt6.setString(1, word1);
		stmt6.setString(2, word2);
		start = System.nanoTime();
		rs = stmt6.executeQuery();
		end = System.nanoTime();
		System.out.println("Most frequent words after " + word1 + " " + word2);
		while (rs.next()) {
			System.out.println(rs.getString("CONTENT"));
		}

		writeToFile(((end - start)/1000000)+"");

		rs.close();
		rs = null;
	}

	public boolean testConnection(String driver, String url, String uname,
			String pwd) throws DataSourceException {
		try {
			conn = getConnection(driver, url, uname, pwd);
		} catch (DataSourceException e) {
			throw e;
		}

		return true;
	}

	public Connection getConnection(String driver, String url, String uname,
			String pwd) throws DataSourceException {
		Connection conn = null;

		try {
			Class.forName(driver).newInstance();
			System.out.println(url);
			System.out.println(uname);
			System.out.println(pwd);
			conn = DriverManager.getConnection(url, uname, pwd);

		} catch (SQLException ex) {
			throw new DataSourceException(
					"Error establishing data source connection: "
							+ ex.getMessage(), ex);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			throw new DataSourceException(
					"Error establishing data source connection: "
							+ e.getMessage(), e);
			// e.printStackTrace();
		}

		return conn;
	}
}
