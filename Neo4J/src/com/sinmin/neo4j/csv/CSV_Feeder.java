package com.sinmin.neo4j.csv;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.*;
import java.util.Iterator;

/**
 * Created by dimuthuupeksha on 10/23/14.
 */
public class CSV_Feeder {
    private static final String DB_PATH = "/Users/dimuthuupeksha/Documents/Academic/FYP/neo4j/data/graph.db";
    GraphDatabaseService graphDb;
    ExecutionEngine engine;
    ExecutionResult result;

    public void addWords(int count) {
        //for (int i = 0; i < count; i++) {
            String root = "/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/";
            String absPath = root + count + "/word.csv";
            String query = "LOAD CSV WITH HEADERS FROM \"file:" + absPath + "\" AS row\n" +
                    "CREATE (:Word {ID: toint(row.ID), VAL: row.VAL, FREQUENCY: toint(row.FREQUENCY)});";

            try (Transaction tx = graphDb.beginTx()) {
                result = engine.execute(query);
                System.out.println(result.dumpToString());
                tx.success();
            }
            System.out.println(absPath);

        //}
    }

    public void addArticles(int count) {
        //for (int i = 0; i < count; i++) {
            String root = "/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/";
            String absPath = root + count + "/article.csv";
            String query = "LOAD CSV WITH HEADERS FROM \"file:" + absPath + "\" AS row\n" +
                    "CREATE (:Article {ID: toint(row.ID), TOPIC: row.TOPIC, AUTHOR: row.AUTHOR, CATEGORY: row.CATEGORY, SUBCAT1: row.SUBCAT1, YEAR: toint(row.YEAR), MONTH: toint(row.MONTH), DAY: toint(row.DAY)});";

            try (Transaction tx = graphDb.beginTx()) {
                result = engine.execute(query);
                System.out.println(result.dumpToString());
                tx.success();
            }
            System.out.println(absPath);

        //}
    }

    public void createIndexOnAtricle_ID() {
        String query = "CREATE INDEX ON :Article(ID);";

        try (Transaction tx = graphDb.beginTx()) {
            result = engine.execute(query);
            System.out.println(result.dumpToString());
            tx.success();
        }
        query = "CREATE INDEX ON :Article(YEAR);";

        try (Transaction tx = graphDb.beginTx()) {
            result = engine.execute(query);
            System.out.println(result.dumpToString());
            tx.success();
        }
        query = "CREATE INDEX ON :Article(MONTH);";

        try (Transaction tx = graphDb.beginTx()) {
            result = engine.execute(query);
            System.out.println(result.dumpToString());
            tx.success();
        }
        query = "CREATE INDEX ON :Article(DAY);";

        try (Transaction tx = graphDb.beginTx()) {
            result = engine.execute(query);
            System.out.println(result.dumpToString());
            tx.success();
        }
        System.out.println("Created Index on Article ID");

    }

    public void createIndexOnWord_ID() {
        String query = "CREATE INDEX ON :Word(ID);";

        try (Transaction tx = graphDb.beginTx()) {
            result = engine.execute(query);
            System.out.println(result.dumpToString());
            tx.success();
        }
        query = "CREATE INDEX ON :Word(VAL);";

        try (Transaction tx = graphDb.beginTx()) {
            result = engine.execute(query);
            System.out.println(result.dumpToString());
            tx.success();
        }
        System.out.println("Created Index on Word ID");

    }

    public void createIndexOnSentence_ID() {
        String query = "CREATE INDEX ON :Sentence(ID);";

        try (Transaction tx = graphDb.beginTx()) {
            result = engine.execute(query);
            System.out.println(result.dumpToString());
            tx.success();
        }
        System.out.println("Created Index on Sentence ID");

    }

    public void addSentence(int count) {
        //for (int i = 0; i < count; i++) {
            String root = "/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/";
            String absPath = root + count + "/sentence.csv";
            String query = "LOAD CSV WITH HEADERS FROM \"file:" + absPath + "\" AS row " +
                    "CREATE (sentence:Sentence {ID: toint(row.ID), WORDS: toint(row.WORDS), POSITION: toint(row.POSITION)}) " +
                    "WITH * " +
                    "MATCH (article:Article {ID: toint(row.ARTICLE_ID)}) " +
                    "MERGE (article)-[:CONTAIN]->(sentence);";
            try (Transaction tx = graphDb.beginTx()) {
                result = engine.execute(query);
                System.out.println(result.dumpToString());
                tx.success();
            }
            System.out.println(absPath);

        //}
    }

    public void addSentence_Word(int count) {
        //long startTime = System.nanoTime();
        try {

            //for (int i = 1; i < count; i++) {
                //PrintWriter writer = new PrintWriter(new FileWriter("times.txt", true));
                //System.out.println(i);
                String root = "/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/";
                String absPath = root + count + "/word_sentence.csv";
                String query = "LOAD CSV WITH HEADERS FROM \"file:" + absPath + "\" AS row " +
                        "MATCH (sentence:Sentence {ID: toint(row.SENTENCE_ID)}) " +
                        "MATCH (word:Word {ID: toint(row.WORD_ID)}) " +
                        "MERGE (sentence)-[:HAS {index : toint(row.POSITION)}]->(word);";
                try (Transaction tx = graphDb.beginTx()) {
                    result = engine.execute(query);
                    System.out.println(result.dumpToString());
                    tx.success();
                }
                System.out.println(absPath);
                //long curTime = System.nanoTime();
                //long gap = (curTime - startTime) / 1000000000;
                //writer.println(gap);
                //writer.flush();
                //writer.close();
            //}

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void resample(String root, String fileName, long sampleRate) throws IOException {
        String sCurrentLine;

        BufferedReader br = new BufferedReader(new FileReader(root + fileName + ".csv"));
        String headers = br.readLine();
        BufferedWriter bw = new BufferedWriter(new FileWriter(root + fileName + "_0.csv"));
        bw.write(headers);
        bw.newLine();
        int fileIndex = 1;
        long lines = 0;
        while ((sCurrentLine = br.readLine()) != null) {
            lines++;
            bw.write(sCurrentLine);
            bw.newLine();
            if (lines > sampleRate) {
                bw.flush();
                bw.close();
                bw = new BufferedWriter(new FileWriter(root + fileName + "_" + fileIndex + ".csv"));
                bw.write(headers);
                bw.newLine();
                fileIndex++;
                lines = 0;
            }
            //System.out.println(sCurrentLine);
        }
        bw.flush();
        bw.close();
        br.close();
    }

    public void emptyCache(){
        graphDb.shutdown();
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        engine = new ExecutionEngine(graphDb);
    }

    public void warmupCache(){
        emptyCache();
        try (Transaction tx = graphDb.beginTx()) {
            GlobalGraphOperations.at(graphDb).getAllNodes();
            tx.success();
        }
        try (Transaction tx = graphDb.beginTx()) {
            GlobalGraphOperations.at(graphDb).getAllRelationships();
            tx.success();
        }


        try{
            Thread.sleep(2000);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    public long executeQuery(String query){
        long totalTime =0;
        try (Transaction tx = graphDb.beginTx()) {
            long startTime = System.nanoTime();
            ExecutionResult result = engine.execute(query);
            long endTime = System.nanoTime();
            totalTime = (endTime-startTime)/1000000;
            System.out.println(result.dumpToString());
            tx.success();
        }
        return totalTime;
    }

    public void executeQueriesWarmUpCache(int file){
        long times[][] = new long[6][17];
        for(int j=0;j<6;j++){
            for(int i=0;i<17;i++){
                if(i==14||i==13){
                    times[j][i] = -1;
                    continue;
                }
                System.out.println("Warmup "+i);
                warmupCache();
                long time = executeQuery(query[i]);
                times[j][i] = time;

            }
        }
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("queryTime_neo4j_warmupcache.txt", true)));
            for (int i = 0; i < 6; i++) {
                String st = file+",";
                for (int j = 0; j < 17; j++) {
                    st= st+(times[i][j]);
                    if(j!=16){
                        st=st+",";
                    }
                }
                out.println(st);
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void executeQueriesColdCache(int file){
        long times[][] = new long[6][17];
        for(int j=0;j<6;j++){
            for(int i=0;i<17;i++){
                if(i==14||i==13){
                    times[j][i] = -1;
                    continue;
                }
                System.out.println("Cold "+i);
                emptyCache();
                long time = executeQuery(query[i]);
                times[j][i] = time;

            }
        }
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("queryTime_neo4j_coldcache.txt", true)));
            for (int i = 0; i < 6; i++) {
                String st = file+",";
                for (int j = 0; j < 17; j++) {
                    st= st+(times[i][j]);
                    if(j!=16){
                        st=st+",";
                    }
                }
                out.println(st);
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void createIndexes(){
        createIndexOnSentence_ID();
        createIndexOnWord_ID();
        createIndexOnAtricle_ID();
    }

    public CSV_Feeder() {
        query =new String[17];
        query[0] = Query1;
        query[1] = Query2;
        query[2] = Query3;
        query[3] = Query4;
        query[4] = Query5;
        query[5] = Query6;
        query[6] = Query7;
        query[7] = Query8;
        query[8] = Query9;
        query[9] = Query10;
        query[10] = Query11;
        query[11] = Query12;
        query[12] = Query13;
        query[13] = Query14;
        query[14] = Query15;
        query[15] = Query16;
        query[16] = Query17;

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        engine = new ExecutionEngine(graphDb);
        //addWords(0);
        //addArticles("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/Article/","ARTICLE_DATA_TABLE",8);

        //addSentence("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/Sentence/", "SENTENCE_DATA_TABLE", 41);
        //createIndexes();
        try{

            long insertTime =0;
            for(int i=0;i<55;i++){
                PrintWriter writer = new PrintWriter(new FileWriter("noe4j_csv_times.txt", true));
                long startTime = System.nanoTime();
                addWords(i);
                addArticles(i);
                addSentence(i);
                addSentence_Word(i);
                long curTime = System.nanoTime();
                long gap = (curTime - startTime) / 1000000000;
                insertTime = insertTime+gap;
                writer.println(insertTime);
                writer.flush();
                writer.close();
                executeQueriesColdCache(i);
                executeQueriesWarmUpCache(i);
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }

        //addSentence_Word("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/Sentence_Words/", "SENTENCE_WORD_DATA_TABLE", 661);
        //try{
        //resample("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/Sentence_Words/","SENTENCE_WORD_DATA_TABLE",5000);
        //}catch(IOException ex){
        //   ex.printStackTrace();
        //}
    }
    String query[];
    public static void main(String as[]) {
        CSV_Feeder feeder = new CSV_Feeder();
    }

    String Query1=
    "match (w:Word) where w.VAL='ජනාධිපති' return w.FREQUENCY";

    String Query2=
    "match (a:Article)-[:CONTAIN]->(s:Sentence)-[:HAS]->(w:Word) where w.VAL='මහින්ද' and a.YEAR=2010 return count(w)";

    String Query3 =
    "match (a:Article)-[:CONTAIN]->(s:Sentence)-[:HAS]->(w:Word) where w.VAL='මහින්ද' and a.YEAR=2011 return count(w)";

    String Query4=
    "match (a:Article)-[:CONTAIN]->(s:Sentence)-[:HAS]->(w:Word) where w.VAL='මහින්ද' and a.YEAR=2012 return count(w)";

    String Query5 =
    "match (a:Article)-[:CONTAIN]->(s:Sentence)-[r:HAS]->(w:Word) where a.YEAR=2010 return count(s),w.VAL order by count(s) desc limit 10";

    String Query6=
    "match (a:Article)-[:CONTAIN]->(s:Sentence)-[r:HAS]->(w:Word) where w.VAL='මහින්ද' return a.ID,a.YEAR,a.MONTH,a.DAY order by a.YEAR,a.MONTH,a.DAY desc limit 10";

    String Query7=
    "match (a:Article)-[:CONTAIN]->(s:Sentence)-[r:HAS]->(w:Word) where w.VAL='මහින්ද' and a.YEAR=2010 return a.ID,a.YEAR,a.MONTH,a.DAY order by a.YEAR,a.MONTH,a.DAY desc limit 10";

    String Query8=
    "match (a:Article)-[:CONTAIN]->(s:Sentence)-[r:HAS]->(w:Word) where w.VAL='මහින්ද' and a.YEAR=2011 return a.ID,a.YEAR,a.MONTH,a.DAY order by a.YEAR,a.MONTH,a.DAY desc limit 10";

    String Query9=
    "match (a:Article)-[:CONTAIN]->(s:Sentence)-[r:HAS]->(w:Word) where w.VAL='මහින්ද' and a.YEAR=2012 return a.ID,a.YEAR,a.MONTH,a.DAY order by a.YEAR,a.MONTH,a.DAY desc limit 10";

    String Query10=
    "match (s:Sentence)-[r:HAS]->(w:Word) where r.index=2 return count(r), w.VAL order by count(r) desc limit 10";

    String Query11=
    "match (w1:Word)<-[r1:HAS]-(s:Sentence)-[r2:HAS]->(w2:Word) where r1.index=r2.index+1 and w1.VAL='මහින්ද' return count(r2), w2.VAL order by count(r2) desc";

    String Query12=
    "match (w1:Word)<-[r1:HAS]-(s:Sentence)-[r2:HAS]->(w2:Word), (a:Article)-[:CONTAIN]->(s) where r1.index=r2.index-1 and w1.VAL='මහින්ද' and w2.VAL='රාජපක්ෂ' and a.YEAR=2010 return count(*)";

    String Query13=
    "match (w1:Word)<-[r1:HAS]-(s:Sentence)-[r2:HAS]->(w2:Word), (s)-[r3:HAS]->(w3:Word), (a:Article)-[:CONTAIN]->(s) where r1.index=r2.index-1 and r2.index=r3.index-1 and w1.VAL='ජනාධිපති' and w2.VAL='මහින්ද' and w3.VAL='රාජපක්ෂ' and a.YEAR=2010 return count(*)";

    String Query14=
    "match (w1:Word)<-[r1:HAS]-(s:Sentence)-[r2:HAS]->(w2:Word) where r1.index=r2.index-1 return count(*),w1.VAL,w2.VAL order by count(*) desc limit 10";

    String Query15=
    "match (w1:Word)<-[r1:HAS]-(s:Sentence)-[r2:HAS]->(w2:Word), (s)-[r3:HAS]->(w3:Word) where r1.index=r2.index-1 and r2.index=r3.index-1 return count(*),w1.VAL,w2.VAL,w3.VAL order by count(*) desc limit 10";

    String Query16=
    "match (w1:Word)<-[r1:HAS]-(s:Sentence)-[r2:HAS]->(w2:Word) where r1.index=r2.index-1 and w1.VAL='මහින්ද' return count(*),w2.VAL order by count(*) desc limit 10";

    String Query17=
    "match (w1:Word)<-[r1:HAS]-(s:Sentence)-[r2:HAS]->(w2:Word), (s)-[r3:HAS]->(w3:Word) where r1.index=r2.index-1 and r2.index=r3.index-1 and w1.VAL='මහින්ද' and w2.VAL='රාජපක්ෂ' return count(*),w3.VAL order by count(*) desc limit 10";


}
