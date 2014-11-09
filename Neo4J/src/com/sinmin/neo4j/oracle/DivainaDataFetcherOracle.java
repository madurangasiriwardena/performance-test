package com.sinmin.neo4j.oracle;

import com.sinmin.neo4j.oracle.bean.ArticleBean;
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
public class DivainaDataFetcherOracle {
    OracleJavaClient client = new OracleJavaClient();

    public static void main(String args[]) {

        //client.addWordToGraph("Heloooo");
        System.out.println("Initializing 11");
        DivainaDataFetcherOracle fetcher = new DivainaDataFetcherOracle();
        System.out.println("Initializing 2");
        fetcher.readFile();
        System.out.println("Initializing 3");
    }


    private void readFile() {
        //client.startTransaction();
        long startTime = System.nanoTime();
        long section1Time = 0;
        long section2Time = 0;
        long section3Time = 0;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            System.out.println("Reading file");
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File("/Users/dimuthuupeksha/Desktop/chamila/SinMinData/Divaina/Small/S0.xml"));
            NodeList nodeList = document.getDocumentElement().getChildNodes();
            System.out.println("Adding data to graph");

            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);

                if (node instanceof Element) {
                    String sents[] = {};
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
                                sents = splitToSentences(content);
                                System.out.println(sents.length);


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
                    ArticleBean bean = new ArticleBean();
                    bean.year = year;
                    bean.month = month;
                    bean.day = day;
                    bean.author = author;
                    bean.topic = topic;
                    Long articleId = client.addArticleToOracle(bean); //adding article
                    if (articleId != 0) {
                        for (int w = 0; w < sents.length; w++) {
                            System.out.println(sents[w].trim());
                            String words[] = splitToWords(sents[w].trim());

                            Long id = client.addSentenceToOracle(words, articleId, w + 1); //adding sentence

                            //sentenceIds.add(id);
                            //for(String word:words){
                            //  client.addWordToGraph(word);
                            // System.out.println(word);
                            //}
                        }
                    }

                    long section3Start = System.nanoTime();

                    System.out.println("Year " + year);
                    System.out.println("Month " + month);
                    System.out.println("Day " + day);
                    System.out.println("Author " + author);
                    System.out.println("Topic " + topic);
                    System.out.println("Article id " + articleId);
                    for (int j = 0; j < sentenceIds.size(); j++) {
                        System.out.println(sentenceIds.get(j));
                    }

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
        //System.out.println("Total add article time "+ client.getAddArticleTime());
        //System.out.println("Total add sentence time "+client.getAddSentenceTime());
        //System.out.println("Section 1 time "+section1Time);
        //System.out.println("Section 2 time "+section2Time);
        //System.out.println("Section 3 time "+section3Time);
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
