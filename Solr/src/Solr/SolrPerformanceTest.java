package Solr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author lahiru
 */
public class SolrPerformanceTest {
    
    String xmlPath;
    String parsedXMLPath;
    String uploadTimeResultFilePath;
    String queryTimeResultFilePath;
    String solrPostJarPath;
    String queries[];

    public SolrPerformanceTest(String parsedXMLPath, String uploadTimeResultFilePath,
            String queryTimeResultFilePath, String solrPostJarPath) {
        this.parsedXMLPath = parsedXMLPath;
        this.uploadTimeResultFilePath = uploadTimeResultFilePath;
        this.queryTimeResultFilePath = queryTimeResultFilePath;
        this.solrPostJarPath = solrPostJarPath;
        
        initQueries();
    }
    
    private void initQueries() {
        queries    = new String[17];
        queries[0] = "http://localhost:8983/solr/collection1/select?q=content:%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF"
                + "&fl=,fl:termfreq(content,%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF)&rows=1000000";
        queries[1] = "http://localhost:8983/solr/collection1/select?q=date:[2010-01-01T00:00:00.000Z%20TO%202010-12-31T23:59:59.999Z]"
                + "&content:%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF&"
                + "fl=,fl:termfreq(content,%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF)&rows=1000000";
        queries[2] = "http://localhost:8983/solr/collection1/select?q=date:[2011-01-01T00:00:00.000Z%20TO%202011-12-31T23:59:59.999Z]"
                + "&content:%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF&fl=,fl:termfreq(content,%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF)&rows=1000000";
        queries[3] = "http://localhost:8983/solr/collection1/select?q=date:[2012-01-01T00:00:00.000Z%20TO%202012-12-31T23:59:59.999Z]"
                + "&content:%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF&fl=,fl:termfreq(content,%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF)&rows=1000000";
        queries[5] = "http://localhost:8983/solr/collection1/select?q=content:%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF"
                + "&sort=date%20desc&rows=50";
        queries[6] = "http://localhost:8983/solr/collection1/select?q=date:[2010-01-01T00:00:00.000Z%20TO%202010-12-31T23:59:59.999Z]"
                + "&content:%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF&sort=date%20desc&rows=50";
        queries[7] = "http://localhost:8983/solr/collection1/select?q=date:[2011-01-01T00:00:00.000Z%20TO%202011-12-31T23:59:59.999Z]"
                + "&content:%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF&sort=date%20desc&rows=50";
        queries[8] = "http://localhost:8983/solr/collection1/select?q=date:[2012-01-01T00:00:00.000Z%20TO%202012-12-31T23:59:59.999Z]"
                + "&content:%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF&sort=date%20desc&rows=50";
        queries[11] = "http://localhost:8983/solr/collection1/select?q=content_shringled:"
                + "%22%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF%20%E0%B6%BB%E0%B7%8F%E0%B6%A2%E0%B6%B4%E0%B6%9A%E0%B7%8A%E0%B7%82%22"
                + "&fl=,fl:termfreq(content_shringled,"
                + "%22%E0%B6%B8%E0%B7%84%E0%B7%92%E0%B6%B1%E0%B7%8A%E0%B6%AF%20%E0%B6%BB%E0%B7%8F%E0%B6%A2%E0%B6%B4%E0%B6%9A%E0%B7%8A%E0%B7%82%22)"
                + "&rows=1000000";
        queries[12] = "http://localhost:8983/solr/collection1/select?q=content_shringled3:"
                + "%22%E0%B6%A2%E0%B6%B1%E0%B7%8F%E0%B6%B0%E0%B7%92%E0%B6%B4%E0%B6%AD%E0%B7%92%20%E0%B6%B8%E0%B7%84%E0%B7"
                + "%92%E0%B6%B1%E0%B7%8A%E0%B6%AF%20%E0%B6%BB%E0%B7%8F%E0%B6%A2%E0%B6%B4%E0%B6%9A%E0%B7%8A%E0%B7%82%22"
                + "&fl=,fl:termfreq(content_shringled3,"
                + "%22%E0%B6%A2%E0%B6%B1%E0%B7%8F%E0%B6%B0%E0%B7%92%E0%B6%B4%E0%B6%AD%E0%B7%92%20%E0%B6%B8%E0%B7%84%E0%B7%92%"
                + "E0%B6%B1%E0%B7%8A%E0%B6%AF%20%E0%B6%BB%E0%B7%8F%E0%B6%A2%E0%B6%B4%E0%B6%9A%E0%B7%8A%E0%B7%82%22)"
                + "&rows=1000000";
    }
    
    public LinkedList<String> getXMLFileList(String directoryPath) {
        LinkedList<String> fileList = new LinkedList<String>();
        File directory = new File(directoryPath);
        for (File file : directory.listFiles()) {
            fileList.addLast(file.getName());
        }
        // sorting the file list
        String temp[] = new String[fileList.size()];
        for(int i = 0; i < temp.length; ++i) {
            temp[i] = fileList.get(i);
        }
        Arrays.sort(temp);
        fileList.clear();
        for(int i = 0; i < temp.length; ++i) {
            fileList.addLast(temp[i]);
        }        
        return fileList;
    }
    
    public void appendUploadTimeResultFile(String s) {
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(uploadTimeResultFilePath, true)));
            out.println(s);
        } catch (IOException ex) {
            Logger.getLogger(SolrPerformanceTest.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            out.close();
        }
    }
    
    public void appendQueryTimeResultFile(String s) {
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(queryTimeResultFilePath, true)));
            out.print(s + ",");
            System.out.print(s + ",");
        } catch (IOException ex) {
            Logger.getLogger(SolrPerformanceTest.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            out.close();
        }
    }
    
    public void appendNewLineAtQueryTImeResultFile() {
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(queryTimeResultFilePath, true)));
            out.println();
            System.out.println();
        } catch (IOException ex) {
            Logger.getLogger(SolrPerformanceTest.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            out.close();
        }
    }
    
    public String encodeURL(String word) throws UnsupportedEncodingException {
        return URLEncoder.encode(word, "UTF-8");
    }

    public int execQuery(String q) {
        long startTime = 0;
        long endTime = -1;
        try {
            String queryString =  q;
            URL query = new URL(queryString);
            startTime = System.nanoTime();
            URLConnection connection = query.openConnection();
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));            
            while (inputStream.readLine() != null); // wait until upload is finished
            inputStream.close();
            endTime = System.nanoTime();
        }  catch(IOException e) {
            Logger.getLogger(SolrPerformanceTest.class.getName()).log(Level.SEVERE, null, e);
        }
        int time = (int) ((endTime - startTime) / 1000000);
        return time;
    }
    
    public void executeAllQueries(int fileNo) {
        for(int i=0; i < 6; ++i) {
            appendQueryTimeResultFile(fileNo + "");
            for(String q : queries) {
                if(q == null || q.equals("")) {
                    appendQueryTimeResultFile("-1");
                }
                else {
                    appendQueryTimeResultFile(execQuery(q) + "");
                }
            }
            appendNewLineAtQueryTImeResultFile();
        }
    }
    
    public void doTestOnEachTestcase() {
        LinkedList<String> xmlFileList = getXMLFileList(parsedXMLPath);
        for(int i = 0; i < xmlFileList.size(); ++i) {
            String xmlFile = parsedXMLPath + xmlFileList.get(i);
            System.out.println("uploading file : " + xmlFile);
            long uploadTime = -1;
            try {
                long startTime = System.currentTimeMillis();
                String command = "java -jar"
                        + " " + solrPostJarPath + " " + xmlFile;
                Process p = Runtime.getRuntime().exec(new String[]{"bash", "-c", command});
                InputStream solrInputStream = p.getInputStream();
                BufferedReader solrStreamReader = new BufferedReader(new InputStreamReader(solrInputStream));
                String line = "";
                // wait for uploading finished
                while ((line = solrStreamReader.readLine ()) != null);
                long endTime = System.currentTimeMillis();
                uploadTime = endTime - startTime;
                appendUploadTimeResultFile(uploadTime + "");
            } catch(Exception ex) {
                Logger.getLogger(SolrPerformanceTest.class.getName()).log(Level.SEVERE, null, ex);
            }
            executeAllQueries(i);
        }
    }
    
    public static void main(String[] args) {
        String parsedXMLPath            = args[0];
        String uploadTimeResultFilePath = args[1];
        String queryTimeResultFilePath  = args[2];
        String solrPostJarPath = "./lib/post.jar";
        
        SolrPerformanceTest test = new SolrPerformanceTest(parsedXMLPath, uploadTimeResultFilePath, 
                queryTimeResultFilePath, solrPostJarPath);
        test.doTestOnEachTestcase();
    }
}
