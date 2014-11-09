package com.sinmin.neo4j.rest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by dimuthuupeksha on 6/20/14.
 */
public class GraphRestClient {
    HttpClient client = new DefaultHttpClient();

    public boolean addWordToGraph(String word) {
        HttpPost post = new HttpPost("http://localhost:7474/db/data/cypher");
        JSONObject obj = new JSONObject();
        post.setHeader("Accept-Language", "en-US,en;q=0.8");
        post.setHeader("Accept-Encoding", "gzip,deflate,sdch");

        try {
            JSONObject props = new JSONObject();
            props.put("value", word);
            JSONObject params = new JSONObject();
            params.put("props", props);

            //obj.put("query", "CREATE (w:Word { props } ) RETURN w");
            obj.put("query", "merge (w:Word {value : \"" + word + "\"}) on create set w.count=1 on match set w.count=w.count+1 return w");
            //obj.put("params", params);
            //"create (s:Sentence) foreach (f in [{value : \"val1\"},{value : \"val3\"}] | merge (w:Word {value : f.value}) on create set w.count=1 on match set w.count=w.count+1 create (s)-[r:c1]->(w)) return s";

            post.setEntity(new StringEntity(obj.toString(), "UTF-8"));
            System.out.println(obj.toString());
            HttpResponse response = client.execute(post);
            printResponse(response);

            return true;
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean removeWordFromGraph(String word) {
        return false;
    }

    public boolean addCharacterToGraph(String character) {
        return false;
    }

    public boolean createCharacterWordMapping(String word, String character, int position) {
        return false;
    }

    public boolean addSentenceToGraph(String words[]) {
        HttpPost post = new HttpPost("http://localhost:7474/db/data/cypher");
        JSONObject obj = new JSONObject();
        post.setHeader("Accept-Language", "en-US,en;q=0.8");
        post.setHeader("Accept-Encoding", "gzip,deflate,sdch");

        JSONObject params = new JSONObject();
        JSONArray wordsArr = new JSONArray();
        try {
            for (int i = 0; i < words.length; i++) {
                String word = words[i];
                JSONObject value = new JSONObject();
                value.put("value", word);
                value.put("index", i);
                wordsArr.put(value);
            }
            params.put("words", wordsArr);

            obj.put("query", "create (s:Sentence) foreach (f in {words} | merge (w:Word {value : f.value}) on create set w.count=1 on match set w.count=w.count+1 create (s)-[r:HAS {index : f.index}]->(w)) return id(s)");
            obj.put("params", params);

            post.setEntity(new StringEntity(obj.toString(), "UTF-8"));
            System.out.println(obj.toString());
            HttpResponse response = client.execute(post);
            printResponse(response);

        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    private void printResponse(HttpResponse response) {
        try {
            HttpEntity entity = response.getEntity();
            String responseString = EntityUtils.toString(entity, "UTF-8");
            System.out.println(responseString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
