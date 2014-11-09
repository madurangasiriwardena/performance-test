package com.sinmin.neo4j.oracle;

import com.sinmin.neo4j.beans.SentenceBean;
import com.sinmin.neo4j.oracle.bean.ArticleBean;
import com.sinmin.neo4j.oracle.bean.Bigram;
import com.sinmin.neo4j.oracle.bean.Trigram;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;
import oracle.jdbc.oracore.OracleType;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import sun.misc.BASE64Encoder;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;

/**
 * Created by dimuthuupeksha on 9/19/14.
 */
public class PLSQLClient {
    private static final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String DB_CONNECTION = "jdbc:oracle:thin:@//localhost:1521/PDB1";
    private static final String DB_USER = "sinmin";
    private static final String DB_PASSWORD = "sinmin";

    //private static final String DB_CONNECTION = "jdbc:oracle:thin:@//192.248.15.239:1522/corpus.sinmin.com";
    //private static final String DB_USER = "SYSTEM";
    //private static final String DB_PASSWORD = "Sinmin1234";

    private static Connection dbConnection = null;

    ArrayList<ArticleBean> articles = null;
    HashMap<String, Long> word_id_map = new HashMap<>();

    private static Connection getDBConnection() {

        if (dbConnection == null) {
            try {
                dbConnection = DriverManager.getConnection(
                        DB_CONNECTION, DB_USER, DB_PASSWORD);
                return dbConnection;
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
        return dbConnection;
    }

    public void addWords(String wordList[]) throws SQLException {
        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := addWords(?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");


        ArrayDescriptor desc = ArrayDescriptor.createDescriptor("WORDARRAY", dbConnection);
        ARRAY wordArray = new ARRAY(desc, dbConnection, wordList);
        stmt.setArray(2, wordArray);
        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        //for (int i = 0; i < values.length; i++)
        //System.out.println("val " + wordList[i] + " = '" + values[i] + "'");

        stmt.close();
        //dbConnection.close();
    }

    public BigDecimal[] getIDList(String wordList[]) throws SQLException {
        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := getWords(?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");


        ArrayDescriptor desc = ArrayDescriptor.createDescriptor("WORDARRAY", dbConnection);
        ARRAY wordArray = new ARRAY(desc, dbConnection, wordList);
        stmt.setArray(2, wordArray);
        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        //for (int i = 0; i < values.length; i++)
        //System.out.println("val " + wordList[i] + " = '" + values[i] + "'");

        stmt.close();
        //dbConnection.close();
        return values;

    }

    public String[] getAllWords(String file) throws IOException, ParserConfigurationException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        System.out.println("Reading file");
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new File("/Users/dimuthuupeksha/Documents/Academic/FYP/temp/out/" + file + ".xml"));
        NodeList nodeList = document.getDocumentElement().getChildNodes();
        ArrayList<String> totWords = new ArrayList<>();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);

            if (node instanceof Element) {
                String sents[] = {};

                NodeList metadata = node.getChildNodes();

                List<Long> sentenceIds = new ArrayList<Long>();

                String day = "", month = "", year = "", topic = "", author = "", link = "", category = "";
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
                        } else if (s.equals("date")) {//System.out.println("Date "+content);
                            NodeList dates = attr.getChildNodes();
                            for (int l = 0; l < dates.getLength(); l++) {
                                if (dates.item(l).getNodeName().equals("year")) {
                                    year = dates.item(l).getTextContent().trim();
                                } else if (dates.item(l).getNodeName().equals("month")) {
                                    month = dates.item(l).getTextContent().trim();
                                } else if (dates.item(l).getNodeName().equals("day")) {
                                    day = dates.item(l).getTextContent().trim();
                                }

                            }

                        } else if (s.equals("topic")) {
                            topic = content;

                        } else if (s.equals("author")) {
                            author = content;

                        } else if (s.equals("link")) {
                            link = content;

                        } else if (s.equals("category")) {
                            category = content;

                        }

                    }
                }

                for (int w = 0; w < sents.length; w++) {
                    //System.out.println(sents[w].trim());
                    String words[] = splitToWords(sents[w].trim());
                    wordcount += words.length;
                    for (int k = 0; k < words.length; k++) {
                        if (word_id_map.get(words[k]) == null) {
                            totWords.add(words[k]);
                        }
                    }
                }

                ArticleBean bean = new ArticleBean();
                bean.year = year;
                bean.month = month;
                bean.day = day;
                bean.author = author;
                bean.topic = topic;
                bean.category = category;
                bean.link = link;
                /*System.out.println("year : "+year);
                System.out.println("month : "+month);
                System.out.println("day : "+day);
                System.out.println("author : "+author);
                System.out.println("topic : "+topic);
                System.out.println("cat : "+category);
                System.out.println("link : "+link);*/

                for (int s = 0; s < sents.length; s++) {
                    //System.out.println(sents[s]);
                    SentenceBean sbean = new SentenceBean();
                    sbean.sentence = sents[s];
                    bean.sentences.add(sbean);
                }
                articles.add(bean);
            }
        }

        String temp[] = new String[totWords.size()];
        temp = totWords.toArray(temp);
        return temp;

    }

    private String[] splitToSentences(String article) {

        return article.split("[\u002E]");
    }

    private String[] splitToWords(String sentence) {
        return sentence.split("[\u0020]");
    }

    public void createDictionary(String file) throws Exception {
        String wordlist[] = getAllWords(file);
        System.out.println("Getting all word Ids");
        BigDecimal[] ids = getIDList(wordlist);
        HashSet<String> missingWords = new HashSet<>();
        for (int i = 0; i < ids.length; i++) {
            if (ids[i].longValue() == 0) {
                missingWords.add(wordlist[i]);
            } else {
                word_id_map.put(wordlist[i], ids[i].longValue());
            }
        }
        String newWords[] = {};
        newWords = missingWords.toArray(newWords);
        System.out.println("Adding new words");
        addWords(newWords);
        System.out.println("Getting new word ids");
        ids = getIDList(newWords);
        for (int i = 0; i < ids.length; i++) {
            word_id_map.put(newWords[i], ids[i].longValue());
        }
        System.out.println("Words " + wordlist.length);
        //System.out.println(ids.length);
        //System.out.println(missingWords.size());
    }

    public BigDecimal[] getBigramIDList(long bigramList[][]) throws SQLException {
        getDBConnection();

        long[] array1 = new long[bigramList.length];
        long[] array2 = new long[bigramList.length];

        for (int i = 0; i < bigramList.length; i++) {
            array1[i] = bigramList[i][0];
            array2[i] = bigramList[i][1];
        }

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := getBigrams(?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");


        ArrayDescriptor desc = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY wordArray1 = new ARRAY(desc, dbConnection, array1);
        ARRAY wordArray2 = new ARRAY(desc, dbConnection, array2);
        stmt.setArray(2, wordArray1);
        stmt.setArray(3, wordArray2);
        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        //for (int i = 0; i < values.length; i++)
        //System.out.println("val " + wordList[i] + " = '" + values[i] + "'");

        stmt.close();
        //dbConnection.close();
        return values;

    }

    public BigDecimal[] getBigramIDList(Bigram bigramList[]) throws SQLException {
        getDBConnection();

        long[] array1 = new long[bigramList.length];
        long[] array2 = new long[bigramList.length];

        for (int i = 0; i < bigramList.length; i++) {
            array1[i] = bigramList[i].id1;
            array2[i] = bigramList[i].id2;
        }

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := getBigrams(?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");


        ArrayDescriptor desc = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY wordArray1 = new ARRAY(desc, dbConnection, array1);
        ARRAY wordArray2 = new ARRAY(desc, dbConnection, array2);
        stmt.setArray(2, wordArray1);
        stmt.setArray(3, wordArray2);
        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        //for (int i = 0; i < values.length; i++)
        //System.out.println("val " + wordList[i] + " = '" + values[i] + "'");

        stmt.close();
        //dbConnection.close();
        return values;

    }

    public void addBigrams(Bigram bigramList[]) throws SQLException {
        long[] array1 = new long[bigramList.length];
        long[] array2 = new long[bigramList.length];

        for (int i = 0; i < bigramList.length; i++) {
            array1[i] = bigramList[i].id1;
            array2[i] = bigramList[i].id2;
        }

        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := addBigrams(?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");


        ArrayDescriptor desc = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY wordArray1 = new ARRAY(desc, dbConnection, array1);
        ARRAY wordArray2 = new ARRAY(desc, dbConnection, array2);
        stmt.setArray(2, wordArray1);
        stmt.setArray(3, wordArray2);
        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        //for (int i = 0; i < values.length; i++)
        //System.out.println("val " + wordList[i] + " = '" + values[i] + "'");

        stmt.close();
        //dbConnection.close();
    }

    public long[][] getAllBigrams() {
        ArrayList<long[]> big = new ArrayList<>();
        for (int i = 0; i < articles.size(); i++) {
            for (int j = 0; j < articles.get(i).sentences.size(); j++) {
                String[] words = splitToWords(articles.get(i).sentences.get(j).sentence);
                for (int k = 0; k < words.length - 1; k++) {
                    long[] temp = {word_id_map.get(words[k]), word_id_map.get(words[k + 1])};
                    if (bigram_map.get(new Bigram(temp[0], temp[1])) == null) {
                        big.add(temp);
                    }
                }
            }
        }
        long[][] bigrams = new long[big.size()][2];
        bigrams = big.toArray(bigrams);
        return bigrams;
    }

    HashMap<Bigram, Long> bigram_map = new HashMap<>();

    public void createBigrams() throws SQLException {
        long[][] bigrams = getAllBigrams();
        BigDecimal[] ids = getBigramIDList(bigrams);
        HashSet<Bigram> missingBigrams = new HashSet<>();
        for (int i = 0; i < ids.length; i++) {
            if (ids[i].intValue() == 0) {
                Bigram misB = new Bigram(bigrams[i][0], bigrams[i][1]);
                missingBigrams.add(misB);
            } else {
                Bigram bi = new Bigram(bigrams[i][0], bigrams[i][1]);
                bigram_map.put(bi, ids[i].longValue());
                // System.out.println(bi.id1+","+bi.id2+","+bi.hashCode());

            }
        }
        Bigram[] newBigrams = new Bigram[missingBigrams.size()];
        newBigrams = missingBigrams.toArray(newBigrams);
        addBigrams(newBigrams);
        ids = getBigramIDList(newBigrams);
        for (int i = 0; i < ids.length; i++) {
            bigram_map.put(newBigrams[i], ids[i].longValue());
        }
        System.out.println("Bigrams created");
        //System.out.println(bigrams.length+","+missingBigrams.size());
    }

    public void addTrigrams(Trigram trigramList[]) throws SQLException {
        long[] array1 = new long[trigramList.length];
        long[] array2 = new long[trigramList.length];
        long[] array3 = new long[trigramList.length];

        for (int i = 0; i < trigramList.length; i++) {
            array1[i] = trigramList[i].id1;
            array2[i] = trigramList[i].id2;
            array3[i] = trigramList[i].id3;
        }

        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := addTrigrams(?,?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");


        ArrayDescriptor desc = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY wordArray1 = new ARRAY(desc, dbConnection, array1);
        ARRAY wordArray2 = new ARRAY(desc, dbConnection, array2);
        ARRAY wordArray3 = new ARRAY(desc, dbConnection, array3);

        stmt.setArray(2, wordArray1);
        stmt.setArray(3, wordArray2);
        stmt.setArray(4, wordArray3);
        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        //for (int i = 0; i < values.length; i++)
        //System.out.println("val " + wordList[i] + " = '" + values[i] + "'");

        stmt.close();
        //dbConnection.close();
    }

    public BigDecimal[] getTrigramIDList(long trigramList[][]) throws SQLException {
        getDBConnection();

        long[] array1 = new long[trigramList.length];
        long[] array2 = new long[trigramList.length];
        long[] array3 = new long[trigramList.length];

        for (int i = 0; i < trigramList.length; i++) {
            array1[i] = trigramList[i][0];
            array2[i] = trigramList[i][1];
            array3[i] = trigramList[i][2];
        }

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := getTrigrams(?,?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");


        ArrayDescriptor desc = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY wordArray1 = new ARRAY(desc, dbConnection, array1);
        ARRAY wordArray2 = new ARRAY(desc, dbConnection, array2);
        ARRAY wordArray3 = new ARRAY(desc, dbConnection, array3);
        stmt.setArray(2, wordArray1);
        stmt.setArray(3, wordArray2);
        stmt.setArray(4, wordArray3);
        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        //for (int i = 0; i < values.length; i++)
        //System.out.println("val " + wordList[i] + " = '" + values[i] + "'");

        stmt.close();
        //dbConnection.close();
        return values;

    }

    public BigDecimal[] getTrigramIDList(Trigram trigramList[]) throws SQLException {
        getDBConnection();

        long[] array1 = new long[trigramList.length];
        long[] array2 = new long[trigramList.length];
        long[] array3 = new long[trigramList.length];

        for (int i = 0; i < trigramList.length; i++) {
            array1[i] = trigramList[i].id1;
            array2[i] = trigramList[i].id2;
            array3[i] = trigramList[i].id3;
        }

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := getTrigrams(?,?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");


        ArrayDescriptor desc = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY wordArray1 = new ARRAY(desc, dbConnection, array1);
        ARRAY wordArray2 = new ARRAY(desc, dbConnection, array2);
        ARRAY wordArray3 = new ARRAY(desc, dbConnection, array3);
        stmt.setArray(2, wordArray1);
        stmt.setArray(3, wordArray2);
        stmt.setArray(4, wordArray3);
        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        //for (int i = 0; i < values.length; i++)
        //System.out.println("val " + wordList[i] + " = '" + values[i] + "'");

        stmt.close();
        //dbConnection.close();
        return values;

    }

    public long[][] getAllTrigrams() {
        ArrayList<long[]> tig = new ArrayList<>();
        for (int i = 0; i < articles.size(); i++) {
            for (int j = 0; j < articles.get(i).sentences.size(); j++) {
                String[] words = splitToWords(articles.get(i).sentences.get(j).sentence);
                for (int k = 0; k < words.length - 2; k++) {
                    long[] temp = {word_id_map.get(words[k]), word_id_map.get(words[k + 1]), word_id_map.get(words[k + 2])};
                    if (trigram_map.get(new Trigram(temp[0], temp[1], temp[2])) == null) {
                        tig.add(temp);
                    }
                }
            }
        }
        long[][] trigrams = new long[tig.size()][3];
        trigrams = tig.toArray(trigrams);
        return trigrams;
    }

    HashMap<Trigram, Long> trigram_map = new HashMap<>();

    public void createTrigrams() throws SQLException {
        long[][] trigrams = getAllTrigrams();
        BigDecimal[] ids = getTrigramIDList(trigrams);
        HashSet<Trigram> missingTrigrams = new HashSet<>();
        for (int i = 0; i < ids.length; i++) {
            if (ids[i].intValue() == 0) {
                Trigram msti = new Trigram(trigrams[i][0], trigrams[i][1], trigrams[i][2]);
                missingTrigrams.add(msti);
            } else {
                Trigram ti = new Trigram(trigrams[i][0], trigrams[i][1], trigrams[i][2]);
                trigram_map.put(ti, ids[i].longValue());
                // System.out.println(bi.id1+","+bi.id2+","+bi.hashCode());

            }
        }
        Trigram[] newTrigrams = new Trigram[missingTrigrams.size()];
        newTrigrams = missingTrigrams.toArray(newTrigrams);
        addTrigrams(newTrigrams);
        ids = getTrigramIDList(newTrigrams);
        for (int i = 0; i < ids.length; i++) {
            trigram_map.put(newTrigrams[i], ids[i].longValue());
        }
        System.out.println("Triigrams created");
        //System.out.println(bigrams.length+","+missingBigrams.size());
    }

    private String[] fetchDate(String dateString) {
        String date[] = dateString.split("/");
        if (date != null && date.length == 3) {
            return date;
        } else {
            return null;
        }
    }

    public void addArticles() throws SQLException {
        String day[] = new String[articles.size()];
        String month[] = new String[articles.size()];
        String year[] = new String[articles.size()];
        String topic[] = new String[articles.size()];
        String author[] = new String[articles.size()];
        String cat[] = new String[articles.size()];
        String subcat[] = new String[articles.size()];

        for (int i = 0; i < articles.size(); i++) {
            day[i] = articles.get(i).day;
            month[i] = articles.get(i).month;
            year[i] = articles.get(i).year;
            topic[i] = articles.get(i).topic;
            author[i] = articles.get(i).author;
            cat[i] = articles.get(i).category;
            subcat[i] = articles.get(i).subCat1;
        }

        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := addArticle(?,?,?,?,?,?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");

//﻿topic_arr in WordArray2,author_arr in WordArray2,cat_arr in WordArray3,subcat_arr in WordArray3,year_arr in NUM_ARRAY,month_arr in NUM_ARRAY,day_arr in NUM_ARRAY
        ArrayDescriptor desc1 = ArrayDescriptor.createDescriptor("WORDARRAY2", dbConnection);
        ARRAY topic_arr = new ARRAY(desc1, dbConnection, topic);
        stmt.setArray(2, topic_arr);

        ArrayDescriptor desc2 = ArrayDescriptor.createDescriptor("WORDARRAY2", dbConnection);
        ARRAY author_arr = new ARRAY(desc2, dbConnection, author);
        stmt.setArray(3, author_arr);

        ArrayDescriptor desc3 = ArrayDescriptor.createDescriptor("WORDARRAY3", dbConnection);
        ARRAY cat_arr = new ARRAY(desc3, dbConnection, cat);
        stmt.setArray(4, cat_arr);

        ArrayDescriptor desc4 = ArrayDescriptor.createDescriptor("WORDARRAY3", dbConnection);
        ARRAY subcat_arr = new ARRAY(desc4, dbConnection, subcat);
        stmt.setArray(5, subcat_arr);

        ArrayDescriptor desc5 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY year_arr = new ARRAY(desc5, dbConnection, year);
        stmt.setArray(6, year_arr);

        ArrayDescriptor desc6 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY month_arr = new ARRAY(desc6, dbConnection, month);
        stmt.setArray(7, month_arr);

        ArrayDescriptor desc7 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY day_arr = new ARRAY(desc7, dbConnection, day);
        stmt.setArray(8, day_arr);


        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        System.out.println("Articles " + values.length);
        for (int i = 0; i < values.length; i++) {
            articles.get(i).id = values[i].longValue();
        }

    }

    public void addSentences() throws SQLException {
        //﻿words_arr in NUM_ARRAY, article_arr in NUM_ARRAY, position_arr in NUM_ARRAY
        ArrayList<Integer> word_count = new ArrayList<>();
        ArrayList<Long> article_id = new ArrayList<>();
        ArrayList<Integer> positions = new ArrayList<>();
        for (int i = 0; i < articles.size(); i++) {
            for (int j = 0; j < articles.get(i).sentences.size(); j++) {
                String sentence = articles.get(i).sentences.get(j).sentence;
                word_count.add(splitToWords(sentence).length);
                article_id.add(articles.get(i).id);
                positions.add(j + 1);
            }
        }
        Integer wordArr[] = {};
        Long idArr[] = {};
        Integer posArr[] = {};
        wordArr = word_count.toArray(wordArr);
        idArr = article_id.toArray(idArr);
        posArr = positions.toArray(posArr);

        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := addSentences(?,?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");

        ArrayDescriptor desc1 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY w_arr = new ARRAY(desc1, dbConnection, wordArr);
        stmt.setArray(2, w_arr);

        ArrayDescriptor desc2 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY a_arr = new ARRAY(desc2, dbConnection, idArr);
        stmt.setArray(3, a_arr);

        ArrayDescriptor desc3 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY p_arr = new ARRAY(desc3, dbConnection, posArr);
        stmt.setArray(4, p_arr);

        stmt.executeUpdate();
        ARRAY output = stmt.getARRAY(1);

        BigDecimal[] values = (BigDecimal[]) output.getArray();

        System.out.println("Sentences " + values.length);
        int sentsId = 0;
        for (int i = 0; i < articles.size(); i++) {
            for (int j = 0; j < articles.get(i).sentences.size(); j++) {
                articles.get(i).sentences.get(j).id = values[sentsId].longValue();
                sentsId++;
            }
        }

    }

    public void createWord_Sentence() throws SQLException {
        ArrayList<Long> sentence = new ArrayList<>();
        ArrayList<Long> word = new ArrayList<>();
        ArrayList<Integer> pos = new ArrayList<>();

        for (int i = 0; i < articles.size(); i++) {
            for (int j = 0; j < articles.get(i).sentences.size(); j++) {
                String s = articles.get(i).sentences.get(j).sentence;
                String arr[] = splitToWords(s);
                for (int k = 0; k < arr.length; k++) {
                    sentence.add(articles.get(i).sentences.get(j).id);
                    word.add(word_id_map.get(arr[k]));
                    pos.add(k + 1);
                }
            }
        }
        //﻿addSentenceWord

        Long wordArr[] = {};
        Long sentArr[] = {};
        Integer posArr[] = {};
        wordArr = word.toArray(wordArr);
        sentArr = sentence.toArray(sentArr);
        posArr = pos.toArray(posArr);

        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := addSentenceWord(?,?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");

        ArrayDescriptor desc1 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY w_arr = new ARRAY(desc1, dbConnection, wordArr);
        stmt.setArray(2, w_arr);

        ArrayDescriptor desc2 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY s_arr = new ARRAY(desc2, dbConnection, sentArr);
        stmt.setArray(3, s_arr);

        ArrayDescriptor desc3 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY p_arr = new ARRAY(desc3, dbConnection, posArr);
        stmt.setArray(4, p_arr);

        stmt.executeUpdate();

    }


    public void createBigram_Sentence() throws SQLException {
        ArrayList<Long> sentence = new ArrayList<>();
        ArrayList<Long> word = new ArrayList<>();
        ArrayList<Integer> pos = new ArrayList<>();

        for (int i = 0; i < articles.size(); i++) {
            for (int j = 0; j < articles.get(i).sentences.size(); j++) {
                String s = articles.get(i).sentences.get(j).sentence;
                String arr[] = splitToWords(s);
                for (int k = 0; k < arr.length - 1; k++) {
                    sentence.add(articles.get(i).sentences.get(j).id);
                    long id1 = word_id_map.get(arr[k]);
                    long id2 = word_id_map.get(arr[k + 1]);
                    long[] tmp = {id1, id2};
                    Bigram bi = new Bigram(tmp[0], tmp[1]);
                    Long id = bigram_map.get(bi);
                    word.add(id);
                    pos.add(k + 1);
                }
            }
        }

        Long wordArr[] = {};
        Long sentArr[] = {};
        Integer posArr[] = {};
        wordArr = word.toArray(wordArr);
        sentArr = sentence.toArray(sentArr);
        posArr = pos.toArray(posArr);

        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := addSentenceBigram(?,?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");

        ArrayDescriptor desc1 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY w_arr = new ARRAY(desc1, dbConnection, wordArr);
        stmt.setArray(2, w_arr);

        ArrayDescriptor desc2 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY s_arr = new ARRAY(desc2, dbConnection, sentArr);
        stmt.setArray(3, s_arr);

        ArrayDescriptor desc3 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY p_arr = new ARRAY(desc3, dbConnection, posArr);
        stmt.setArray(4, p_arr);

        stmt.executeUpdate();


    }

    public void createTrigram_Sentence() throws SQLException {
        ArrayList<Long> sentence = new ArrayList<>();
        ArrayList<Long> word = new ArrayList<>();
        ArrayList<Integer> pos = new ArrayList<>();

        for (int i = 0; i < articles.size(); i++) {
            for (int j = 0; j < articles.get(i).sentences.size(); j++) {
                String s = articles.get(i).sentences.get(j).sentence;
                String arr[] = splitToWords(s);
                for (int k = 0; k < arr.length - 2; k++) {
                    sentence.add(articles.get(i).sentences.get(j).id);
                    long id1 = word_id_map.get(arr[k]);
                    long id2 = word_id_map.get(arr[k + 1]);
                    long id3 = word_id_map.get(arr[k + 2]);
                    long[] tmp = {id1, id2, id3};
                    Trigram ti = new Trigram(tmp[0], tmp[1], tmp[2]);
                    Long id = trigram_map.get(ti);
                    //System.out.println(id1+","+id2+","+id3+","+id);
                    word.add(id);
                    pos.add(k + 1);
                }
            }
        }

        Long wordArr[] = {};
        Long sentArr[] = {};
        Integer posArr[] = {};
        wordArr = word.toArray(wordArr);
        sentArr = sentence.toArray(sentArr);
        posArr = pos.toArray(posArr);

        getDBConnection();

        OracleCallableStatement stmt = (OracleCallableStatement) dbConnection.prepareCall("begin ? := addSentenceTrigram(?,?,?); end;");
        stmt.registerOutParameter(1, OracleTypes.ARRAY, "IDARRAY");

        ArrayDescriptor desc1 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY w_arr = new ARRAY(desc1, dbConnection, wordArr);
        stmt.setArray(2, w_arr);

        ArrayDescriptor desc2 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY s_arr = new ARRAY(desc2, dbConnection, sentArr);
        stmt.setArray(3, s_arr);

        ArrayDescriptor desc3 = ArrayDescriptor.createDescriptor("NUM_ARRAY", dbConnection);
        ARRAY p_arr = new ARRAY(desc3, dbConnection, posArr);
        stmt.setArray(4, p_arr);

        stmt.executeUpdate();
    }

    public void flushCache() throws SQLException {
        getDBConnection();
        Statement stmt = dbConnection.createStatement();
        //int rst = stmt.executeUpdate("alter system flush shared_pool");
        //stmt = dbConnection.createStatement();
        int rst = stmt.executeUpdate("alter system flush buffer_cache");
        stmt.close();
    }

    public void runQueries(String file) throws SQLException {
        long queryTime[][] = new long[6][17];
        long startTime, endTime;
        for (int it = 0; it < 6; it++) {
            //////////////
            flushCache();
            getDBConnection();
            Statement stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            ResultSet rst = stmt.executeQuery("select frequency from word where Word.val='මහින්ද'");
            endTime = System.nanoTime();
            queryTime[it][0] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("frequency"));
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select count(*) from word,sentence_word,SENTENCE,ARTICLE where Word.val='මහින්ද' and word.id=sentence_word.word_id and sentence.id=sentence_word.sentence_id and article.id = sentence.article_id and article.year=2010");
            endTime = System.nanoTime();
            queryTime[it][1] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("count(*)"));
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select count(*) from word,sentence_word,SENTENCE,ARTICLE where Word.val='මහින්ද' and word.id=sentence_word.word_id and sentence.id=sentence_word.sentence_id and article.id = sentence.article_id and article.year=2011");
            endTime = System.nanoTime();
            queryTime[it][2] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("count(*)"));
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select count(*) from word,sentence_word,SENTENCE,ARTICLE where Word.val='මහින්ද' and word.id=sentence_word.word_id and sentence.id=sentence_word.sentence_id and article.id = sentence.article_id and article.year=2012");
            endTime = System.nanoTime();
            queryTime[it][3] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("count(*)"));
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select word.val,count(article.id) from word,sentence_word,SENTENCE,ARTICLE where word.id=sentence_word.word_id and sentence.id=sentence_word.sentence_id and article.id = sentence.article_id and article.year=2010 GROUP BY word.val ORDER BY count(article.id) desc) where rownum <= 5");
            endTime = System.nanoTime();
            queryTime[it][4] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("val").trim() + "," + rst.getString("count(article.id)").trim());
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select article.year,article.month,article.day from word,sentence_word,SENTENCE,ARTICLE where Word.val='මහින්ද' and word.id=sentence_word.word_id and sentence.id=sentence_word.sentence_id and article.id = sentence.article_id ORDER BY article.year desc,month desc,day desc) where rownum <= 5");
            endTime = System.nanoTime();
            queryTime[it][5] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("year").trim() + "," + rst.getString("month").trim() + "," + rst.getString("day").trim());
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select article.year,article.month,article.day,article.id from word,sentence_word,SENTENCE,ARTICLE where Word.val='මහින්ද' and word.id=sentence_word.word_id and sentence.id=sentence_word.sentence_id and article.id = sentence.article_id and year =2010 ORDER BY article.year desc,month desc,day desc) where rownum <= 5");
            endTime = System.nanoTime();
            queryTime[it][6] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("year").trim() + "," + rst.getString("month").trim() + "," + rst.getString("day").trim() + "," + rst.getString("id").trim());
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select article.year,article.month,article.day,article.id from word,sentence_word,SENTENCE,ARTICLE where Word.val='මහින්ද' and word.id=sentence_word.word_id and sentence.id=sentence_word.sentence_id and article.id = sentence.article_id and year =2011 ORDER BY article.year desc,month desc,day desc) where rownum <= 5");
            endTime = System.nanoTime();
            queryTime[it][7] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("year").trim() + "," + rst.getString("month").trim() + "," + rst.getString("day").trim() + "," + rst.getString("id").trim());
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select article.year,article.month,article.day,article.id from word,sentence_word,SENTENCE,ARTICLE where Word.val='මහින්ද' and word.id=sentence_word.word_id and sentence.id=sentence_word.sentence_id and article.id = sentence.article_id and year =2012 ORDER BY article.year desc,month desc,day desc) where rownum <= 5");
            endTime = System.nanoTime();
            queryTime[it][8] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("year").trim() + "," + rst.getString("month").trim() + "," + rst.getString("day").trim() + "," + rst.getString("id").trim());
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select word.val, count(*) from word,sentence_word where word.id=sentence_word.word_id and sentence_word.position=4 GROUP BY word.val ORDER BY count(*) desc) where rownum <= 5");
            endTime = System.nanoTime();
            queryTime[it][9] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("val").trim() + "," + rst.getString("count(*)").trim());
            }
            stmt.close();
            rst.close();

            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select w2.val,count(*) from word w1,word w2,sentence_word sw1,sentence_word sw2 where sw1.sentence_id=sw2.sentence_id and w1.id=sw1.word_id and w2.id=sw2.word_id and w1.val='මහින්ද' and sw2.position=sw1.position-1 group by w2.val order by count(*) desc) where rownum <= 5");
            endTime = System.nanoTime();
            queryTime[it][10] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("val").trim() + "," + rst.getString("count(*)").trim());
            }
            stmt.close();
            rst.close();
            ////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select count(*) from word w1,word w2,bigram, sentence_bigram bs, sentence s, article a where w1.val='ජනාධිපති' and w2.val='මහින්ද' and bigram.word1=w1.id and bigram.word2=w2.id and bs.bigram_id=bigram.id and s.id = bs.sentence_id and s.article_id=a.id and a.year=2010");
            endTime = System.nanoTime();
            queryTime[it][11] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("count(*)").trim());
            }
            stmt.close();
            rst.close();
            ///////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select count(*) from word w1,word w2,word w3,trigram, sentence_trigram bs, sentence s, article a where w1.val='ජනාධිපති' and w2.val='මහින්ද' and w3.val='රාජපක්ෂ' and trigram.word1=w1.id and trigram.word2=w2.id and trigram.word3=w3.id and bs.trigram_id=trigram.id and s.id = bs.sentence_id and s.article_id=a.id and a.year=2010");
            endTime = System.nanoTime();
            queryTime[it][12] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString("count(*)").trim());
            }
            stmt.close();
            rst.close();
            //////////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select w1.val,w2.val, bf frequency from (select b.id, word1,word2, b.frequency as bf from bigram b order by b.frequency desc),word w1, word w2 where rownum <=10 and w1.id=word1 and w2.id=word2");
            endTime = System.nanoTime();
            queryTime[it][13] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString(1).trim() + "," + rst.getString(2).trim() + "," + rst.getString(3).trim());
            }
            stmt.close();
            rst.close();
            ///////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select w1.val,w2.val,w3.val, bf frequency from (select b.id, word1,word2,word3, b.frequency as bf from trigram b order by b.frequency desc),word w1, word w2,word w3 where rownum <=10 and w1.id=word1 and w2.id=word2 and w3.id=word3");
            endTime = System.nanoTime();
            queryTime[it][14] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString(1).trim() + "," + rst.getString(2).trim() + "," + rst.getString(3).trim() + "," + rst.getString(4).trim());
            }
            stmt.close();
            rst.close();
            ///////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select w2.val,b.frequency from bigram b,word w1, word w2 where w1.val='මහින්ද' and b.word1=w1.id and b.word2=w2.id order by b.frequency desc) where rownum <=10");
            endTime = System.nanoTime();
            queryTime[it][15] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString(1).trim() + "," + rst.getString(2).trim());
            }
            stmt.close();
            rst.close();
            ///////////
            flushCache();
            stmt = dbConnection.createStatement();
            startTime = System.nanoTime();
            rst = stmt.executeQuery("select * from (select w3.val,b.frequency from trigram b,word w1, word w2, word w3 where w1.val='මහින්ද' and w2.val='රාජපක්ෂ' and b.word1=w1.id and b.word2=w2.id and b.word3=w3.id order by b.frequency desc) where rownum <=10");
            endTime = System.nanoTime();
            queryTime[it][16] = endTime - startTime;
            while (rst.next()) {
                System.out.println(rst.getString(1).trim() + "," + rst.getString(2).trim());
            }
            stmt.close();
            rst.close();

        }
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("queryTime.txt", true)));
            for (int i = 0; i < 6; i++) {
                String st = file+",";
                for (int j = 0; j < 17; j++) {
                    st= st+(queryTime[i][j] / 1000000);
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

    HashMap<Long, Long> times = new HashMap<>();
    long wordcount = 0;
    long totalTime =0;
    public PLSQLClient() {

        try {
            for (int i = 0; i < 200; i++) {
                long startTime = System.nanoTime();
                articles = new ArrayList<>();
                System.out.println("Creating Dictionary "+i);
                createDictionary(i + "");
                System.out.println("Creating bigrams");
                createBigrams();
                System.out.println("Creating trigrams");
                createTrigrams();
                System.out.println("Adding articles");
                addArticles();
                System.out.println("Adding Sentences");
                addSentences();
                System.out.println("Create Word_sentence join");
                createWord_Sentence();
                System.out.println("Create Bigram_Sentence join");
                createBigram_Sentence();
                System.out.println("Create Trigram_Sentence join");
                createTrigram_Sentence();
                System.out.println("Done");
                long endTime = System.nanoTime();
                totalTime += (endTime-startTime)/1000000000;
                System.out.println("Time "+wordcount +" : "+totalTime );
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("oracleBatchTime.txt", true)));
                out.println(wordcount+","+totalTime);
                out.flush();
                out.close();
                runQueries(i+"");
            }

            //flushCache();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        /*Set<Long> keySet = times.keySet();
        Iterator<Long> it = keySet.iterator();
        while(it.hasNext()){
            Long key = it.next();
            System.out.println(key+" : "+times.get(key));
        }*/
    }

    public static void main(String args[]) {
        new PLSQLClient();
    }
}