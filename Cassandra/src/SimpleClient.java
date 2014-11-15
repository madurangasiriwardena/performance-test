import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.OMXMLBuilderFactory;
import org.apache.axiom.om.OMXMLParserWrapper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SimpleClient {
   private Cluster cluster;
   private Session session;
   public void connect(String node) {
      cluster = Cluster.builder()
            .addContactPoint(node).build();
      Metadata metadata = cluster.getMetadata();
      System.out.printf("Connected to cluster: %s\n", 
            metadata.getClusterName());
      for ( Host host : metadata.getAllHosts() ) {
         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
               host.getDatacenter(), host.getAddress(), host.getRack());
      }
      session = cluster.connect();
   }

   public void close() {
   }

   public static void main(String[] args) {
	   long start, end, time = 0;
      SimpleClient client = new SimpleClient();
      client.connect("127.0.0.1");
      
      String baseFolder = "/home/chamila/semester7/fyp/20_Million_Words/out";
      String file;
      String createMovieCql;
      PreparedStatement insertPost;
      long count = 0;
      //client.runSelectQueries();
      for(int a=0; a<200; a++){
    	  file = baseFolder + "/" + a + ".xml";
    	  System.out.println(file);
    	  InputStream in;
		try {
			start = System.currentTimeMillis();
			in = new FileInputStream(file);
			OMXMLParserWrapper oMXMLParserWrapper = OMXMLBuilderFactory.createOMBuilder(in);
			OMElement root = oMXMLParserWrapper.getDocumentElement();

			Iterator<?> postItr = root.getChildElements();
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
					yearInt = Integer.parseInt(client.trim(year));
				} catch (Exception e) {
					
				}
				try {
					dayInt = Integer.parseInt(client.trim(day));
				} catch (Exception e) {
					
				}
				try {
					monthInt = Integer.parseInt(client.trim(month));
				} catch (Exception e) {

				}
				String timestamp = month + "/" + day + "/" + year;
				DateFormat df = new SimpleDateFormat("mm/dd/yyyy");
				Date date=null;
				try {
					date = df.parse(timestamp);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//System.out.println(date);
				// u002E period
				// u003F question mark
				// u0021 exclamation mark
				// u0020 space
				// u002C comma
				String[] sentences = content.split("[\u002E\u003F\u0021]");
				
				for (int i = 0; i < sentences.length; i++) {
					String[] words = sentences[i].split("[\u0020\u002C]");
					
					for(int j=0;j<words.length;j++){
						//System.out.println(timestamp);
						if(words[j].length()>0){
							//System.out.println(words[j] + " " + count + " ");
							insertPost = client.session.prepare(
										"INSERT INTO sinmin.word_yearly_usage (id,content, sentence, position, postname, year, day, month, date, url, author, topic, category) values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
							client.session.execute(insertPost.bind(count,words[j],sentences[i],j,topic,yearInt,dayInt,monthInt,date,link,author,topic,1));
							//System.out.println("1 right");
							insertPost = client.session.prepare(
									"INSERT INTO sinmin.word_usage (id,content, sentence,date) values (?,?,?,?)");
							client.session.execute(insertPost.bind(count,words[j],sentences[i],date));
							//System.out.println("2 right");
							insertPost = client.session.prepare(
									"select * from sinmin.word_frequency WHERE content=?");
							ResultSet results = client.session.execute(insertPost.bind(words[j]));
							//System.out.println("3 right");
							Row row = results.one();
							if(row==null){
								//System.out.println("4b");
								insertPost = client.session.prepare("INSERT INTO sinmin.word_frequency(id, content, frequency) values (?,?,?)");
								client.session.execute(insertPost.bind(count, words[j],1));
								//System.out.println("4b right");
							}
							else{
								//System.out.println("4a");
								insertPost = client.session.prepare(
										"UPDATE sinmin.word_frequency SET frequency = ? WHERE content=?");
								client.session.execute(insertPost.bind(row.getInt("frequency") + 1,words[j]));
								//System.out.println("4a right");
							}
							//////////////////////////////////////////////////////////////////////////////////////////
							insertPost = client.session.prepare(
									"select * from sinmin.word_time_frequency WHERE content=? AND year=?");
							results = client.session.execute(insertPost.bind(words[j],yearInt));
							//System.out.println("5 right");
							row = results.one();
							if(row==null){
								//System.out.println("6a");
								insertPost = client.session.prepare("INSERT INTO sinmin.word_time_frequency(id, content, year, frequency) values (?,?,?,?)");
								client.session.execute(insertPost.bind(count, words[j],yearInt,1));
								insertPost = client.session.prepare("INSERT INTO sinmin.word_time_inv_frequency(id, content, year, frequency) values (?,?,?,?)");
								client.session.execute(insertPost.bind(count, words[j],yearInt,1));
								//System.out.println("6a right");
							}
							else{
								//System.out.println("6b");
								insertPost = client.session.prepare(
										"UPDATE sinmin.word_time_frequency SET frequency = ? WHERE content=? AND year=?");
								client.session.execute(insertPost.bind(row.getInt("frequency") + 1,words[j],yearInt));
								insertPost = client.session.prepare(
										"DELETE FROM sinmin.word_time_inv_frequency WHERE content=? AND year=? AND frequency = ? ");
								client.session.execute(insertPost.bind(words[j],yearInt,row.getInt("frequency")));
								insertPost = client.session.prepare(
										"INSERT INTO sinmin.word_time_inv_frequency(id, content, year, frequency) values (?,?,?,?)");
								client.session.execute(insertPost.bind(count, words[j],yearInt,row.getInt("frequency")+1));
								///System.out.println("6b right");
							}
							
							////////////////////////////////////////
							insertPost = client.session.prepare(
									"select * from sinmin.word_pos_id WHERE position=? AND content = ?");
							results = client.session.execute(insertPost.bind(j,words[j]));
							//System.out.println("7 right");
							row = results.one();
							if(row==null){
								//System.out.println("8b");
								insertPost = client.session.prepare("INSERT INTO sinmin.word_pos_frequency(id, content, position,frequency) values (?,?,?,?)");
								client.session.execute(insertPost.bind(count, words[j],j,1));
								insertPost = client.session.prepare("INSERT INTO sinmin.word_pos_id(id, content, position,frequency) values (?,?,?,?)");
								client.session.execute(insertPost.bind(count, words[j],j,1));
								//System.out.println("8b right");
							}
							else{
								//System.out.println("8a");
								
								insertPost = client.session.prepare(
										"UPDATE sinmin.word_pos_id SET frequency = ? WHERE content=? AND position=?");
								client.session.execute(insertPost.bind(row.getInt("frequency") + 1,words[j],j));
								insertPost = client.session.prepare(
										"DELETE FROM sinmin.word_pos_frequency WHERE content=? AND position=? AND frequency = ? ");
								client.session.execute(insertPost.bind(words[j],j,row.getInt("frequency")));
								insertPost = client.session.prepare(
										"INSERT INTO sinmin.word_pos_frequency(id, content, position, frequency) values (?,?,?,?)");
								client.session.execute(insertPost.bind(count, words[j],j,row.getInt("frequency")+1));
								//System.out.println("8a right");
							}
							
							//bigram tables
							
							if(j<words.length-1){
								String contentbi = words[j] + " "+words[j+1];
								insertPost = client.session.prepare(
										"select * from sinmin.bigram_id WHERE content = ?");
								results = client.session.execute(insertPost.bind(contentbi));
								//System.out.println("3 right");
								row = results.one();
								if(row==null){
									//System.out.println("4b");
									insertPost = client.session.prepare("INSERT INTO sinmin.bigram_frequency(id, content, frequency, category) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count, contentbi,1,1));
									insertPost = client.session.prepare("INSERT INTO sinmin.bigram_id(id, content, frequency) values (?,?,?)");
									client.session.execute(insertPost.bind(count, contentbi,1));
									//System.out.println("4b right");
								}
								else{
									//System.out.println("4a");
									
									insertPost = client.session.prepare(
											"UPDATE sinmin.bigram_id SET frequency = ? WHERE content=?");
									client.session.execute(insertPost.bind(row.getInt("frequency") + 1,contentbi));
									insertPost = client.session.prepare(
											"DELETE FROM sinmin.bigram_frequency WHERE content=? AND category=? AND frequency = ? ");
									client.session.execute(insertPost.bind(contentbi,1,row.getInt("frequency")));
									insertPost = client.session.prepare(
											"INSERT INTO sinmin.bigram_frequency(id, content, category, frequency) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count, contentbi,1,row.getInt("frequency") + 1));
									
									//System.out.println("4a right");
								}
								
								
								insertPost = client.session.prepare(
										"select * from sinmin.bigram_time_frequency WHERE bigram=? AND year=?");
								results = client.session.execute(insertPost.bind(contentbi,yearInt));
								//System.out.println("3 right");
								row = results.one();
								if(row==null){
									//System.out.println("4b");
									insertPost = client.session.prepare("INSERT INTO sinmin.bigram_time_frequency(id, bigram, year, frequency) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count, contentbi,yearInt,1));
									//System.out.println("4b right");
								}
								else{
									//System.out.println("4a");
									insertPost = client.session.prepare(
											"UPDATE sinmin.bigram_time_frequency SET frequency = ? WHERE bigram=? AND year=?");
									client.session.execute(insertPost.bind(row.getInt("frequency") + 1,contentbi,yearInt));
									//System.out.println("4a right");
								}
								
								
								insertPost = client.session.prepare(
										"select * from sinmin.bigram_with_word_id WHERE word1=? AND word2=?");
								results = client.session.execute(insertPost.bind(words[j],words[j+1]));
								//System.out.println("3 right");
								row = results.one();
								if(row==null){
									//System.out.println("4b");
									insertPost = client.session.prepare("INSERT INTO sinmin.bigram_with_word_id(id, word1, word2, frequency) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count, words[j],words[j+1],1));
									insertPost = client.session.prepare("INSERT INTO sinmin.bigram_with_word_frequency(id, word1, word2, frequency) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count, words[j],words[j+1],1));
									//System.out.println("4b right");
								}
								else{
									//System.out.println("4a");
									insertPost = client.session.prepare(
											"UPDATE sinmin.bigram_with_word_id SET frequency = ? WHERE word1=? AND word2=?");
									client.session.execute(insertPost.bind(row.getInt("frequency") + 1,words[j],words[j+1]));
									insertPost = client.session.prepare(
											"DELETE FROM sinmin.bigram_with_word_frequency WHERE word1=? AND word2=? AND frequency = ? ");
									client.session.execute(insertPost.bind(words[j],words[j+1],row.getInt("frequency")));
									insertPost = client.session.prepare(
											"INSERT INTO sinmin.bigram_with_word_frequency(id, word1, word2, frequency) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count,words[j],words[j+1],row.getInt("frequency") + 1));
									//System.out.println("4a right");
								}
								
								
							}
							
							if(j<words.length-2){
								String contenttri = words[j] + " "+words[j+1] + " "+words[j+2];
								
								insertPost = client.session.prepare(
										"select * from sinmin.trigram_id WHERE content = ?");
								//results = client.session.execute(insertPost.bind(contenttri));
								//System.out.println("3 right");
								row = results.one();
								if(row==null){
									//System.out.println("4b");
									insertPost = client.session.prepare("INSERT INTO sinmin.trigram_frequency(id, content, frequency, category) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count, contenttri,1,1));
									insertPost = client.session.prepare("INSERT INTO sinmin.trigram_id(id, content, frequency) values (?,?,?)");
									client.session.execute(insertPost.bind(count, contenttri,1));
									//System.out.println("4b right");
								}
								else{
									//System.out.println("4a");
									
									insertPost = client.session.prepare(
											"UPDATE sinmin.trigram_id SET frequency = ? WHERE content=?");
									client.session.execute(insertPost.bind(row.getInt("frequency") + 1,contenttri));
									insertPost = client.session.prepare(
											"DELETE FROM sinmin.trigram_frequency WHERE content=? AND category=? AND frequency = ? ");
									client.session.execute(insertPost.bind(contenttri,1,row.getInt("frequency")));
									insertPost = client.session.prepare(
											"INSERT INTO sinmin.trigram_frequency(id, content, category, frequency) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count, contenttri,1,row.getInt("frequency") + 1));
									
									//System.out.println("4a right");
								}
								
								
								insertPost = client.session.prepare(
										"select * from sinmin.trigram_time_frequency WHERE trigram=? AND year=?");
								results = client.session.execute(insertPost.bind(contenttri,yearInt));
								//System.out.println("3 right");
								row = results.one();
								if(row==null){
									//System.out.println("4b");
									insertPost = client.session.prepare("INSERT INTO sinmin.trigram_time_frequency(id, trigram, year, frequency) values (?,?,?,?)");
									client.session.execute(insertPost.bind(count, contenttri,yearInt,1));
									//System.out.println("4b right");
								}
								else{
									//System.out.println("4a");
									insertPost = client.session.prepare(
											"UPDATE sinmin.trigram_time_frequency SET frequency = ? WHERE trigram=? AND year=?");
									client.session.execute(insertPost.bind(row.getInt("frequency") + 1,contenttri,yearInt));
									//System.out.println("4a right");
								}
								
								
								insertPost = client.session.prepare(
										"select * from sinmin.trigram_with_word_id WHERE word1=? AND word2=? AND word3=?");
								results = client.session.execute(insertPost.bind(words[j],words[j+1],words[j+2]));
								//System.out.println("3 right");
								row = results.one();
								if(row==null){
									//System.out.println("4b");
									insertPost = client.session.prepare("INSERT INTO sinmin.trigram_with_word_id(id, word1, word2,word3, frequency) values (?,?,?,?,?)");
									client.session.execute(insertPost.bind(count, words[j],words[j+1],words[j+2],1));
									insertPost = client.session.prepare("INSERT INTO sinmin.trigram_with_word_frequency(id, word1, word2,word3, frequency) values (?,?,?,?,?)");
									client.session.execute(insertPost.bind(count, words[j],words[j+1],words[j+2],1));
									//System.out.println("4b right");
								}
								else{
									//System.out.println("4a");
									insertPost = client.session.prepare(
											"UPDATE sinmin.trigram_with_word_id SET frequency = ? WHERE word1=? AND word2=? AND word3=?");
									client.session.execute(insertPost.bind(row.getInt("frequency") + 1,words[j],words[j+1],words[j+2]));
									insertPost = client.session.prepare(
											"DELETE FROM sinmin.trigram_with_word_frequency WHERE word1=? AND word2=? AND word3=? AND frequency = ? ");
									client.session.execute(insertPost.bind(words[j],words[j+1],words[j+2],row.getInt("frequency")));
									insertPost = client.session.prepare(
											"INSERT INTO sinmin.trigram_with_word_frequency(id, word1, word2,word3, frequency) values (?,?,?,?,?)");
									client.session.execute(insertPost.bind(count,words[j],words[j+1],words[j+2],row.getInt("frequency") + 1));
									//System.out.println("4a right");
								}
								
								
							}
							
							count ++;
						}
					}
					
				}
				
			}
			end = System.currentTimeMillis();
			
			time += (end - start);
			
			try {
				PrintWriter out = new PrintWriter(new BufferedWriter(
						new FileWriter("/home/chamila/semester7/fyp/results/Insert.txt",
								true)));
				out.append(a + " : " + time + "\n");
				out.close();
				System.out.println(a + " : " + time
						+ "ms\n");
			} catch (IOException e) {
				// exception handling left as an exercise for the reader
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(int i=0; i<6; i++){
			client.writeToFile(i+"");
			client.runSelectQueries();  
		}
      }
      
      
//      final String createMovieCql =  
//    		     "CREATE TABLE sinmin.bigram (id int, sentence_id int, start_position int, word_index1 int, word_index2 int, content varchar, year int, category int, "  
//    		   + "PRIMARY KEY (id))";
//      PreparedStatement insertPost = client.session
//				.prepare(
//						"INSERT INTO sinmin.post (id,author,catogary,day,month,title,url,year) values (?,?,?,?,?,?,?,?)");
//      client.session.execute(insertPost.bind(2,"Author",1,4,11,"රියදුරු නින්දට මොරටු සරසවියෙන් බේතක්","http",2014));
    		//client.session.execute(createMovieCql);  
    		//client.session.
      //client.session.execute(createMovieCql);
      client.close();
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
   
   public void writeToFile(String s){
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("/home/chamila/semester7/fyp/results/Query.txt", true)));
			out.append(s);
			out.close();
			System.out.println(s);
		} catch (IOException e) {
			
		}
   }
   
   public void runSelectQueries(){
	   long start, end;
	   
	   PreparedStatement query;
	   //Q1
	   query = this.session.prepare(
				"select frequency from sinmin.word_frequency WHERE content=?");
	   start = System.nanoTime();
	   ResultSet results = this.session.execute(query.bind("මහින්ද"));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
	   //System.out.println("Q1");
	   //System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%d\n", row.getInt("frequency"));
//		    
//		   }
	   //Q2
	   query = this.session.prepare(
				"select frequency from sinmin.word_time_frequency WHERE content=? AND year=?");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද",2010));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q2");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%d\n", row.getInt("frequency"));
//		    
//		   }
	   //Q3
	   query = this.session.prepare(
				"select frequency from sinmin.word_time_frequency WHERE content=? AND year=?");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද",2011));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q3");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%d\n", row.getInt("frequency"));
//		    
//		   }
	   //Q4
	   query = this.session.prepare(
				"select frequency from sinmin.word_time_frequency WHERE content=? AND year=?");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද",2012));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q4");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%d\n", row.getInt("frequency"));
//		    
//		   }
	   
	   //Q5
	   query = this.session.prepare(
				"select content from sinmin.word_time_inv_frequency WHERE year=? ORDER BY frequency DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind(2010));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q5");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("content"));
//		    
//		   }
	   
	   //Q6
	   query = this.session.prepare(
				"select sentence from sinmin.word_usage WHERE content=? ORDER BY date DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද"));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q6");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("sentence"));
//		    
//		   }
	   //Q7
	   query = this.session.prepare(
				"select sentence from sinmin.word_yearly_usage WHERE content=? AND year=? AND category IN ( 1,2,3,4,5)ORDER BY date DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද",2010));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q7");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("sentence"));
//		    
//		   }
	   
	   //Q8
	   query = this.session.prepare(
				"select sentence from sinmin.word_yearly_usage WHERE content=? AND year=? AND category IN ( 1,2,3,4,5) ORDER BY date DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද",2011));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q8");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("sentence"));
//		    
//		   }
	   
	 //Q9
	   query = this.session.prepare(
				"select sentence from sinmin.word_yearly_usage WHERE content=? AND year=? AND category IN ( 1,2,3,4,5) ORDER BY date DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද",2012));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q9");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("sentence"));
//		    
//		   }
	   
	   //Q10
	   query = this.session.prepare(
				"select content from sinmin.word_pos_frequency WHERE position=? ORDER BY frequency DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind(2));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q10");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("content"));
//		    
//		   }
	   
	   //Q11
	   query = this.session.prepare(
				"select word2 from sinmin.bigram_with_word_frequency WHERE word1 = ? ORDER BY frequency DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද"));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q11");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("word2"));
//		    
//		   }
	   
	   //Q12
	   query = this.session.prepare(
				"select frequency from sinmin.bigram_time_frequency WHERE bigram = ? AND year = ?");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද රාජපක්ෂ",2010));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q12");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%d\n", row.getInt("frequency"));
//		    
//		   }
	   
	   //Q13
	   query = this.session.prepare(
				"select frequency from sinmin.trigram_time_frequency Where trigram = ? AND year = ?");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("ජනාධිපති මහින්ද රාජපක්ෂ",2010));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q13");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%d\n", row.getInt("frequency"));
//		    
//		   }
	   //Q14
	   query = this.session.prepare(
				"select content from sinmin.trigram_frequency WHERE category IN ( 1,2,3,4,5) ORDER BY frequency DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind());
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q14");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("content"));
//		    
//		   }
	   
	   //Q15
	   query = this.session.prepare(
				"select content from sinmin.trigram_frequency WHERE category IN ( 1,2,3,4,5) ORDER BY frequency DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind());
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q15");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("content"));
//		    
//		   }
	   
	   //Q16
	   query = this.session.prepare(
				"select word2 from sinmin.bigram_with_word_frequency WHERE word1 = ? ORDER BY frequency DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද"));
	   end = System.nanoTime();
	   writeToFile(","+(end-start)*1.0/1000000);
//	   System.out.println("Q16");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("word2"));
//		    
//		   }
	   
	   //Q17
	   query = this.session.prepare(
				"select word3 from sinmin.trigram_with_word_frequency WHERE word1 = ? AND word2 = ? ORDER BY frequency DESC LIMIT 10");
	   start = System.nanoTime();
	   results = this.session.execute(query.bind("මහින්ද","රාජපක්ෂ"));
	   end = System.nanoTime();
	   writeToFile(","+((end-start)*1.0/1000000) + "\n");
//	   System.out.println("Q17");
//	   System.out.println(results);
//	   for (Row row : results) {
//		   System.out.format("%s\n", row.getString("word3"));
//		    
//		   }
	   System.out.println("pppppppppppppppp");
   }
}