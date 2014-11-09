package com.sinmin.neo4j.rest;

import com.sinmin.neo4j.beans.DivainaArticleBean;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dimuthuupeksha on 6/20/14.
 */
public class DivainaDataFetcher {
    GraphJavaClient client = new GraphJavaClient();

    public static void main(String args[]) {

        //client.addWordToGraph("Heloooo");
        System.out.println("Initializing 1");
        DivainaDataFetcher fetcher = new DivainaDataFetcher();
        System.out.println("Initializing 2");
        for (int i = 5; i < 10; i++) {
            System.out.println("Processing Doc " + i);
            fetcher.readFile(i);
        }
        System.out.println("Finalizing");
    }


    private void readFile(int docId) {
        //client.startTransaction();
        long startTime = System.nanoTime();
        long section1Time = 0;
        long section2Time = 0;
        long section3Time = 0;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            System.out.println("Reading file");
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File("/Users/dimuthuupeksha/Desktop/chamila/SinMinData/Divaina/Small/S" + docId + ".xml"));
            NodeList nodeList = document.getDocumentElement().getChildNodes();
            System.out.println("Adding data to graph of Doc " + docId);

            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);

                if (node instanceof Element) {
                    long section1Start = System.nanoTime();

                    NodeList metadata = node.getChildNodes();
                    //System.out.println(i + " Element");
                    String day = "", month = "", year = "", topic = "", author = "";
                    List<Long> sentenceIds = new ArrayList<Long>();

                    long section1End = System.nanoTime();
                    section1Time += (section1End - section1Start);

                    for (int j = 0; j < metadata.getLength(); j++) {
                        long section2Start = System.nanoTime();
                        Node attr = metadata.item(j);

                        if (attr instanceof Element) {
                            Node lastChild = attr.getLastChild();
                            String content = "";
                            if (lastChild != null) {
                                content = lastChild.getTextContent().trim();
                            }
                            String s = attr.getNodeName();
                            if (s.equals("content")) {
                                String sents[] = splitToSentences(content);
                                //System.out.println(sents.length);
                                for (int w = 0; w < sents.length; w++) {
                                    //System.out.println(sents[w].trim());
                                    String words[] = splitToWords(sents[w].trim());

                                    long section2IgnoreStart = System.nanoTime();
                                    Long id = client.addSentenceToGraph(words);
                                    long section2IgnoreStop = System.nanoTime();

                                    section2Time -= (section2IgnoreStop - section2IgnoreStart);
                                    sentenceIds.add(id);
                                    //for(String word:words){
                                    //  client.addWordToGraph(word);
                                    // System.out.println(word);
                                    //}
                                }

                            } else if (s.equals("date")) {//System.out.println("Date "+content);
                                String dates[] = fetchDate(content);
                                if (dates != null) {
                                    day = dates[0];
                                    month = dates[1];
                                    year = dates[2];
                                }

                            } else if (s.equals("topic")) {
                                topic = content;

                            } else if (s.equals("author")) {
                                author = content;

                            }
                            //System.out.println(attr.getNodeName() + " - " + content);

                        }

                        long section2Stop = System.nanoTime();
                        section2Time += (section2Stop - section2Start);

                    }
                    DivainaArticleBean bean = new DivainaArticleBean();
                    bean.year = year;
                    bean.month = month;
                    bean.day = day;
                    bean.author = author;
                    bean.topic = topic;
                    bean.sentenceIds = sentenceIds;
                    Long articleId = client.addArticleToGraph(bean);

                    long section3Start = System.nanoTime();

                    //System.out.println("Year "+year);
                    //System.out.println("Month "+month);
                    //System.out.println("Day "+day);
                    //System.out.println("Author " + author);
                    //System.out.println("Topic " + topic);
                    //System.out.println("Article id " + articleId);
                    //for(int j=0;j<sentenceIds.size();j++){
                    //  System.out.println(sentenceIds.get(j));
                    //}

                    long section3Stop = System.nanoTime();
                    section3Time += (section3Stop - section3Start);

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        //client.successTransaction();
        long endTime = System.nanoTime();
        System.out.println("Total Running time " + (endTime - startTime));
        System.out.println("Total add article time " + client.getAddArticleTime());
        System.out.println("Total add sentence time " + client.getAddSentenceTime());
        System.out.println("Section 1 time " + section1Time);
        System.out.println("Section 2 time " + section2Time);
        System.out.println("Section 3 time " + section3Time);
    }

    private String[] splitToSentences(String article) {

        return article.split("[\u002E]");
    }

    private String[] splitToWords(String sentence) {
        return sentence.split("[\u0020]");
    }

    private String[] fetchDate(String dateString) {
        String date[] = dateString.split("/");
        if (date != null && date.length == 3) {
            return date;
        } else {
            return null;
        }
    }
}
