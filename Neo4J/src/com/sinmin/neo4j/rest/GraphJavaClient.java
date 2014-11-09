package com.sinmin.neo4j.rest;

import com.sinmin.neo4j.beans.DivainaArticleBean;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.util.*;

/**
 * Created by dimuthuupeksha on 6/24/14.
 */
public class GraphJavaClient {
    private static final String DB_PATH = "/Users/dimuthuupeksha/Documents/Academic/FYP/neo4j/data/graph.db";
    GraphDatabaseService graphDb;
    ExecutionEngine engine;
    ExecutionResult result;
    private long addArticleTime= 0;
    private long addSentenceTime =0;

    public GraphJavaClient(){
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        // END SNIPPET: startDb
        // START SNIPPET: transaction
        engine = new ExecutionEngine(graphDb);


    }

    public long getAddArticleTime() {
        return addArticleTime;
    }

    public long getAddSentenceTime() {
        return addSentenceTime;
    }

    public Long addArticleToGraph(DivainaArticleBean bean){
        long startTime = System.nanoTime();
        String query =  "create (a:Article {props}) return id(a)";
        Long id;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Map<String,Object> params = new HashMap<>();
            Map<String,Object> props = new HashMap<>();
            if(bean.author!=null&&!bean.author.equals(""))
                props.put("author",bean.author);
            if(bean.topic!=null&&!bean.topic.equals(""))
                props.put("topic",bean.topic);
            if(bean.year!=null&&!bean.year.equals(""))
                props.put("year",bean.year);
            if(bean.month!=null&&!bean.month.equals(""))
                props.put("month",bean.month);
            if(bean.day!=null&&!bean.day.equals(""))
                props.put("day",bean.day);
            List<Map<String, Object>> maps = Arrays.asList(props);
            params.put("props",maps);

            result = engine.execute(query,params);
            Iterator<Long> ids=  result.columnAs("id(a)");
            id= ids.next();

            query ="match (a:Article),(s:Sentence) where id(a)="+id+" and id(s) in {arr} create (a)-[:CONTAIN]->(s)";
            params = new HashMap<>();
            params.put("arr",bean.sentenceIds);
            engine.execute(query,params);

            tx.success();
        }
        long endTime = System.nanoTime();
        addArticleTime += (endTime-startTime);
        return id;
    }

    public Long addSentenceToGraph(String words[]){
        long startTime = System.nanoTime();
        Map<String,Object> params = new HashMap<>();
        List<Map<String,Object>> wordsArr = new ArrayList<>();
        for (int i=0;i< words.length;i++) {
            String word = words[i];
            Map<String,Object> value = new HashMap<>();
            value.put("value", word);
            value.put("index",i);
            wordsArr.add(value);
        }
        params.put("words", wordsArr);

        String query = "create (s:Sentence) foreach (f in {words} | merge (w:Word {value : f.value}) on create set w.count=1 on match set w.count=w.count+1 create (s)-[r:HAS {index : f.index}]->(w)) return id(s)";
        //String query = "create (n:Word) return n";
        Long id;
        try ( Transaction tx = graphDb.beginTx() )
        {
            result = engine.execute(query,params);
            Iterator<Long> ids=  result.columnAs("id(s)");
            id= ids.next();
            tx.success();
        }
        long endTime = System.nanoTime();
        //System.out.println("Time "+(endTime-startTime));
        addSentenceTime += (endTime-startTime);
        return id;
    }
}
