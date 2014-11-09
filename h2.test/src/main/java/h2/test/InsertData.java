package h2.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.OMXMLBuilderFactory;
import org.apache.axiom.om.OMXMLParserWrapper;
import org.h2.tools.DeleteDbFiles;

public class InsertData {
	Connection conn = null;
	Statement stmt = null;
	ResultSet rs = null;
	String driverName;
	String url;
	String uname;
	String pwd;

	public static void main(String[] args) throws OMException, IOException,
			ClassNotFoundException, SQLException {
		String baseFolder = "/home/lahiru/Desktop/out";
		if(args.length == 1){
			baseFolder = args[0];
		}else{
			
		}
		InsertData loadDriver = new InsertData();
		loadDriver.driverSimulator(baseFolder);
		// loadDriver.test();
	}

	public void driverSimulator(String baseFolder) throws OMException, IOException {
		String driverName = "org.h2.Driver";
		String url = "jdbc:h2:tcp://localhost/test2";
		String uname="maduranga";
		String pwd="maduranga";

		try {
			boolean valiedConnection = testConnection(driverName,url,uname,pwd);
			if (valiedConnection) {
				System.out.println("Connection is Healthy");
				insertData(baseFolder);
				
			}
		} catch (DataSourceException e) {
			System.out.println(e.getMessage());
		}

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
	
	public ArrayList<File> listFolder(File folder) {
		ArrayList<File> folders = new ArrayList<>();
	    for (File fileEntry : folder.listFiles()) {
	    	folders.add(fileEntry);
	    }
	    
	    return folders;
	}

	public void insertData(String base) throws OMException, IOException {
		stmt = null;
		rs = null;

		long start, end, time =0;
		
		try {
			PrintWriter out = new PrintWriter(
					new BufferedWriter(
							new FileWriter(
									"/home/lahiru/h2/Insert.txt",
									true)));
			out.append("\nNormal\n");
			out.close();
			System.out.println("Normal\n");
		} catch (IOException e) {
			// exception handling left as an exercise for the reader
		}
		
		String baseFolder = base;
//		ArrayList<File> folders = listFolder(new File(baseFolder));
//		for(int a=0; a<folders.size(); a++){
//			System.out.println(folders.get(a).getAbsolutePath());
//			ArrayList<File> files = listFolder(folders.get(a));
//			for(int b=0; b<files.size(); b++) {
//				System.out.println(files.get(b).getAbsolutePath());
//				String file = files.get(b).getAbsolutePath();
		
		for(int a=0; a<200; a++){
			String file = baseFolder + "/" + a + ".xml";
				try {
					
					start = System.currentTimeMillis();
					
					InputStream in = new FileInputStream(file);
					OMXMLParserWrapper oMXMLParserWrapper = OMXMLBuilderFactory
							.createOMBuilder(in);
					OMElement root = oMXMLParserWrapper.getDocumentElement();

					Iterator<?> postItr = root.getChildElements();
					// start = System.currentTimeMillis();
					while (postItr.hasNext()) {
						OMElement post = (OMElement) postItr.next();

						String content = post.getFirstChildWithName(
								new QName("content")).getText();
						String link = post.getFirstChildWithName(new QName("link"))
								.getText();
						String topic = post.getFirstChildWithName(new QName("topic"))
								.getText();
						String day = post.getFirstChildWithName(new QName("date"))
								.getFirstChildWithName(new QName("day")).getText();
						String month = post.getFirstChildWithName(new QName("date"))
								.getFirstChildWithName(new QName("month")).getText();
						String year = post.getFirstChildWithName(new QName("date"))
								.getFirstChildWithName(new QName("year")).getText();
						String author = post.getFirstChildWithName(new QName("author"))
								.getText();
						String category = post.getFirstChildWithName(new QName("category"))
								.getText();

						int yearInt = 0;
						int dayInt = 0;
						int monthInt = 0;
						try {
							yearInt = Integer.parseInt(trim(year));
						} catch (Exception e) {
							
						}
						try {
							dayInt = Integer.parseInt(trim(day));
						} catch (Exception e) {
							
						}
						try {
							monthInt = Integer.parseInt(trim(month));
						} catch (Exception e) {

						}

						stmt = conn.createStatement(
								java.sql.ResultSet.TYPE_FORWARD_ONLY,
								java.sql.ResultSet.CONCUR_UPDATABLE);

						int postIndex = -1;
						int checkValuePost = selectStatement("POST", "POST_INDEX",
								link, "URL");
						if (checkValuePost != -1) {
							postIndex = checkValuePost;
						} else {
							PreparedStatement insertPost = conn
									.prepareStatement(
											"INSERT INTO POST (DAY,MONTH,YEAR,URL,AUTHOR,TOPIC,CATEGORY) values (?,?,?,?,?,?,?)",
											Statement.RETURN_GENERATED_KEYS);
							insertPost.setLong(1, dayInt);
							insertPost.setLong(2, monthInt);
							insertPost.setLong(3, yearInt);
							insertPost.setString(4, link);
							insertPost.setString(5, author);
							insertPost.setString(6, topic);
							insertPost.setString(7, category);
							insertPost.executeUpdate();

							rs = insertPost.getGeneratedKeys();

							if (rs.next()) {
								postIndex = rs.getInt(1);
							} else {
							}

							rs.close();
							rs = null;

							// u002E period
							// u003F question mark
							// u0021 exclamation mark
							// u0020 space
							// u002C comma
							String[] sentences = content.split("[\u002E\u003F\u0021]");

							int sentencePosition = 0;
							for (int i = 0; i < sentences.length; i++) {

								sentences[i] = trim(sentences[i]);
								if (sentences[i].equals("")) {
									continue;
								}
								sentencePosition++;
								int sentenceIndex = -1;

								PreparedStatement insertSentence = conn
										.prepareStatement(
												"INSERT INTO SENTENCE (CONTENT, POST_INDEX, POSITION, WORD_COUNT) values (?,?,?,?)",
												Statement.RETURN_GENERATED_KEYS);
								insertSentence.setString(1, sentences[i]);
								insertSentence.setLong(2, postIndex);
								insertSentence.setLong(3, sentencePosition);
								insertSentence.setLong(4, sentences[i].length());
								insertSentence.executeUpdate();

								rs = insertSentence.getGeneratedKeys();

								if (rs.next()) {
									sentenceIndex = rs.getInt(1);
								} else {
								}

								rs.close();
								rs = null;

								String[] words = sentences[i].split("[\u0020\u002C]");

								int wordPosition = 0;
								int previousWordIndex = -1;
								int previousWord = 0;
								int previousWordIndex2 = -1;
								int previousWord2 = 0;
								int currentWord = 0;
								int wordIndex = -1;
								for (int j = 0; j < words.length; j++) {
									words[j] = trim(words[j]);
									if (words[j].equals("")) {
										continue;
									}
									wordPosition++;
									previousWord2 = previousWord;
									previousWord = currentWord;
									currentWord = j;
									
									previousWordIndex2 = previousWordIndex;
									previousWordIndex = wordIndex;

									wordIndex = -1;
									int frequency = -1;
									String s = "SELECT WORD_INDEX, FREQUENCY FROM WORD WHERE CONTENT = ?";
									PreparedStatement stmt6 = conn.prepareStatement(s);
									stmt6.setString(1, words[j]);
									rs = stmt6.executeQuery();

									while (rs.next()) {
										wordIndex = rs.getInt("WORD_INDEX");
										frequency = rs.getInt("FREQUENCY");
									}

									if (wordIndex != -1) {
										PreparedStatement updateWord = conn
												.prepareStatement("UPDATE WORD SET FREQUENCY=? WHERE WORD_INDEX=?");
										updateWord.setLong(1, frequency + 1);
										updateWord.setLong(2, wordIndex);
										updateWord.execute();

									} else {
										PreparedStatement insertWord = conn
												.prepareStatement(
														"INSERT INTO WORD (CONTENT, FREQUENCY) values (?,?)",
														Statement.RETURN_GENERATED_KEYS);
										insertWord.setString(1, words[j]);
										insertWord.setLong(2, 1);
										insertWord.executeUpdate();

										rs = insertWord.getGeneratedKeys();

										if (rs.next()) {
											wordIndex = rs.getInt(1);
										} else {
										}

										rs.close();
										rs = null;
									}
									PreparedStatement insertSentenceWord = conn
											.prepareStatement("INSERT INTO SENTENCE_WORD (POSITION, SENTENCE_INDEX, WORD_INDEX) values (?,?,?)");
									insertSentenceWord.setLong(1, wordPosition);
									insertSentenceWord.setLong(2, sentenceIndex);
									insertSentenceWord.setLong(3, wordIndex);
									insertSentenceWord.execute();
									
									if(wordPosition>1){
										int bigramIndex = -1;
										int bigramFrequency = -1;
										
										String sBigramSelect = "SELECT BIGRAM_INDEX, FREQUENCY FROM BIGRAM WHERE WORD1_INDEX = ? AND WORD2_INDEX = ?";
										PreparedStatement bigramSelect = conn.prepareStatement(sBigramSelect);
										bigramSelect.setLong(1, previousWordIndex);
										bigramSelect.setLong(2, wordIndex);
										rs = bigramSelect.executeQuery();
										
										while (rs.next()) {
											bigramIndex = rs.getInt("BIGRAM_INDEX");
											bigramFrequency = rs.getInt("FREQUENCY");
										}
										
										if (bigramIndex != -1) {
											PreparedStatement updateBigram = conn
													.prepareStatement("UPDATE BIGRAM SET FREQUENCY=? WHERE BIGRAM_INDEX=?");
											updateBigram.setLong(1, bigramFrequency + 1);
											updateBigram.setLong(2, bigramIndex);
											updateBigram.execute();

										} else {
											PreparedStatement insertBigram = conn
													.prepareStatement(
															"INSERT INTO BIGRAM (CONTENT, WORD1_INDEX, WORD2_INDEX, FREQUENCY) values (?,?,?,?)",
															Statement.RETURN_GENERATED_KEYS);
											insertBigram.setString(1, words[previousWord]+" "+words[j]);
											insertBigram.setLong(2, previousWordIndex);
											insertBigram.setLong(3, wordIndex);
											insertBigram.setLong(4, 1);
											insertBigram.executeUpdate();

											rs = insertBigram.getGeneratedKeys();

											if (rs.next()) {
												bigramIndex = rs.getInt(1);
											} else {
											}

											rs.close();
											rs = null;
										}
										PreparedStatement insertSentenceBigram = conn
												.prepareStatement("INSERT INTO SENTENCE_BIGRAM (POSITION, SENTENCE_INDEX, BIGRAM_INDEX) values (?,?,?)");
										insertSentenceBigram.setLong(1, wordPosition-1);
										insertSentenceBigram.setLong(2, sentenceIndex);
										insertSentenceBigram.setLong(3, bigramIndex);
										insertSentenceBigram.execute();
									}
									
									if(wordPosition>2){
										int trigramIndex = -1;
										int trigramFrequency = -1;
										
										String sTrigramSelect = "SELECT TRIGRAM_INDEX, FREQUENCY FROM TRIGRAM WHERE WORD1_INDEX = ? AND WORD2_INDEX = ? AND WORD3_INDEX = ?";
										PreparedStatement trigramSelect = conn.prepareStatement(sTrigramSelect);
										trigramSelect.setLong(1, previousWordIndex2);
										trigramSelect.setLong(2, previousWordIndex);
										trigramSelect.setLong(3, wordIndex);
										rs = trigramSelect.executeQuery();
										
										while (rs.next()) {
											trigramIndex = rs.getInt("TRIGRAM_INDEX");
											trigramFrequency = rs.getInt("FREQUENCY");
										}
										
										if (trigramIndex != -1) {
											PreparedStatement updateBigram = conn
													.prepareStatement("UPDATE TRIGRAM SET FREQUENCY=? WHERE TRIGRAM_INDEX=?");
											updateBigram.setLong(1, trigramFrequency + 1);
											updateBigram.setLong(2, trigramIndex);
											updateBigram.execute();

										} else {
											PreparedStatement insertTrigram = conn
													.prepareStatement(
															"INSERT INTO TRIGRAM (CONTENT, WORD1_INDEX, WORD2_INDEX, WORD3_INDEX, FREQUENCY) values (?,?,?,?,?)",
															Statement.RETURN_GENERATED_KEYS);
											insertTrigram.setString(1, words[previousWord2]+" "+words[previousWord]+" "+words[j]);
											insertTrigram.setLong(2, previousWordIndex2);
											insertTrigram.setLong(3, previousWordIndex);
											insertTrigram.setLong(4, wordIndex);
											insertTrigram.setLong(5, 1);
											insertTrigram.executeUpdate();

											rs = insertTrigram.getGeneratedKeys();

											if (rs.next()) {
												trigramIndex = rs.getInt(1);
											} else {
											}

											rs.close();
											rs = null;
										}
										PreparedStatement insertSentenceTrigram = conn
												.prepareStatement("INSERT INTO SENTENCE_TRIGRAM (POSITION, SENTENCE_INDEX, TRIGRAM_INDEX) values (?,?,?)");
										insertSentenceTrigram.setLong(1, wordPosition-2);
										insertSentenceTrigram.setLong(2, sentenceIndex);
										insertSentenceTrigram.setLong(3, trigramIndex);
										insertSentenceTrigram.execute();
									}

								}
							}
						}

					}
					end = System.currentTimeMillis();
					
					int count  = 0;
					
					String s = "SELECT COUNT(*) AS COUNT FROM SENTENCE_WORD";
					PreparedStatement ps = conn.prepareStatement(s);
					rs = ps.executeQuery();
					
					if (rs.next()) {
						count = rs.getInt("COUNT");
					}
					
					rs.close();
					rs = null;
					
					time += (end - start);
					
					try {
						PrintWriter out = new PrintWriter(new BufferedWriter(
								new FileWriter("/home/lahiru/h2/Insert.txt",
										true)));
						out.append(count + " : " + time + "\n");
						out.close();
						System.out.println(count + " : " + time
								+ "ms\n");
					} catch (IOException e) {
						// exception handling left as an exercise for the reader
					}
					
					FetchData fd = new FetchData();
					fd.driverSimulator(conn,a);

				} catch (SQLException e) {
					e.printStackTrace();
				} finally {

					if (rs != null) {
						try {
							rs.close();
						} catch (SQLException ex) {
							// ignore
						}
					}

					if (stmt != null) {
						try {
							stmt.close();
						} catch (SQLException ex) {
							// ignore
						}
					}
				}
			}		
//		}
/*
		for(int k=8; k<=8; k++){
			fileName = k+"";
		
		}
		*/
	}

	public String trim(String s) {
		int len = s.length();
		int st = 0;

		while ((st < len)
				&& (s.charAt(st) == '\u00a0' || s.charAt(st) == ' ' || s
						.charAt(st) == '\u0020')) {
			st++;
		}
		while ((st < len)
				&& (s.charAt(len - 1) == '\u00a0' || s.charAt(st) == ' ' || s
						.charAt(st) == '\u0020')) {
			len--;
		}
		return ((st > 0) || (len < s.length())) ? s.substring(st, len) : s;
	}

	public int selectStatement(String table, String columnSelect,
			String content, String columnWhere) {
		int index = -1;
		try {
			String s = "SELECT " + columnSelect + " FROM " + table + " WHERE "
					+ columnWhere + " = ?";
			PreparedStatement stmt6 = conn.prepareStatement(s);
			stmt6.setString(1, content);
			rs = stmt6.executeQuery();

			while (rs.next()) {
				index = rs.getInt(columnSelect);
			}
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				} // ignore

				rs = null;
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				} // ignore

				stmt = null;
			}
		}

		return index;
	}

	public void testConnection() {
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT 1");

			while (rs.next()) {
				String coffeeName = rs.getString("1");

				System.out.println(coffeeName);
			}
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
			// it is a good idea to release
			// resources in a finally{} block
			// in reverse-order of their creation
			// if they are no-longer needed

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				} // ignore

				rs = null;
			}

			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				} // ignore

				stmt = null;
			}
		}
	}

	public void test() throws ClassNotFoundException, SQLException {
		DeleteDbFiles.execute("~", "test", true);

		Class.forName("org.h2.Driver");
		Connection conn = DriverManager
				.getConnection("jdbc:h2://localhost:8082/test_db");
		Statement stat = conn.createStatement();

		// this line would initialize the database
		// from the SQL script file 'init.sql'
		// stat.execute("runscript from 'init.sql'");

		// stat.execute("create table test(id int primary key, name varchar(255))");
		stat.execute("insert into test values(2, 'Hello')");
		ResultSet rs;
		rs = stat.executeQuery("select * from test");
		while (rs.next()) {
			System.out.println(rs.getString("name"));
		}

		Properties p = conn.getClientInfo();
		Set<?> set = p.keySet();

		Iterator<?> iter = set.iterator();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
		stat.close();
		conn.close();
	}

}