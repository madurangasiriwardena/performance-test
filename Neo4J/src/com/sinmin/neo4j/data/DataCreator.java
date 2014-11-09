package com.sinmin.neo4j.data;

import com.sinmin.neo4j.beans.SentenceBean;
import com.sinmin.neo4j.oracle.bean.ArticleBean;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dimuthuupeksha on 10/28/14.
 */
public class DataCreator {
    int wordcount =0;

    TransformerFactory tFactory;
    StreamResult result;
    Transformer transformer;
    Document outDoc;
    Element root;
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder;
    int totfiles =144;

    public void splitDoc(String file) throws Exception{




        System.out.println("Reading file");

        Document document = builder.parse(new File(file));
        NodeList nodeList = document.getDocumentElement().getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);

            if (node instanceof Element) {
                String sents[] = {};

                NodeList metadata = node.getChildNodes();

                List<Long> sentenceIds = new ArrayList<Long>();

                String day = "", month = "", year = "", topic = "", author = "",category="",link="";
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


                        } else if (s.equals("topic")) {
                            topic = content;

                        } else if (s.equals("author")) {
                            author = content;

                        } else if (s.equals("category")) {
                            category = content;

                        } else if (s.equals("link")) {
                            link = content;

                        }

                    }
                }

                for (int w = 0; w < sents.length; w++) {
                    //System.out.println(sents[w].trim());
                    String words[] = splitToWords(sents[w].trim());
                    wordcount+= words.length;
                }

                Node importedNode = outDoc.importNode(node,true);
                root.appendChild(importedNode);

                if(wordcount>=100000){

                    outDoc.appendChild(root);
                    DOMSource source = new DOMSource(outDoc);
                    File f = new File("/Users/dimuthuupeksha/Documents/Academic/FYP/temp/out/"+totfiles+".xml");
                    System.out.println("Writing to "+f.toString());
                    OutputStream outFile= new FileOutputStream(f);
                    result = new StreamResult(outFile);
                    transformer.transform(source, result);

                    totfiles++;
                    outDoc = builder.newDocument();
                    root = outDoc.createElement("root");
                    wordcount =0;
                }


            }
        }


    }
    private String[] splitToSentences(String article) {

        return article.split("[\u002E]");
    }
    private String[] splitToWords(String sentence) {
        return sentence.split("[\u0020]");
    }

    public DataCreator(){
        try{
            builder = factory.newDocumentBuilder();
            tFactory = TransformerFactory.newInstance();
            transformer = tFactory.newTransformer();
            outDoc = builder.newDocument();
            root = outDoc.createElement("root");
            File folder = new File("/Users/dimuthuupeksha/Documents/Academic/FYP/temp/11");
            File[] listOfFiles = folder.listFiles();

            for(int i=0;i<listOfFiles.length;i++){
                System.out.println(listOfFiles[i]);
                splitDoc(listOfFiles[i].toString());
            }

            outDoc.appendChild(root);
            DOMSource source = new DOMSource(outDoc);
            OutputStream outFile= new FileOutputStream("/Users/dimuthuupeksha/Documents/Academic/FYP/temp/out/"+totfiles+".xml");
            result = new StreamResult(outFile);
            transformer.transform(source, result);

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }


    public static void main(String as[]){
        new DataCreator();
    }
}
