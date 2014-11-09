package com.sinmin.neo4j.csv;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dimuthuupeksha on 11/4/14.
 */
public class CSV_Creator {
    PrintWriter word,sentence,word_sentence,article,bigram_sentence,bigram,trigram,trigram_sentence;
    BufferedReader wordReader,sentenceReader,articleReader,sentence_wordReader,bigramSentReader,bigramReader,trigramSentReader,trigramReader;
    long maxWord=0,maxSentence=0,maxArticle=0;
    long currentWord =0,currentSentence=0,currentArticle=0;
    HashMap<String,String> wordMap = new HashMap<>();
    HashMap<String,String> sentenceMap = new HashMap<>();
    HashMap<String,String> articleMap = new HashMap<>();
    HashMap<String,List<String>> bigram_sents_map = new HashMap<>();
    HashMap<String,List<String>> trigram_sents_map = new HashMap<>();
    HashMap<String,String> bigramMap = new HashMap<>();
    HashMap<String,String> trigramMap = new HashMap<>();

    public void write_word(String input){
        word.println(input);
    }
    public void write_bigram(String input){
        bigram.println(input);
    }
    public void write_trigram(String input){
        trigram.println(input);
    }
    public void write_to_sentence(String input){
        sentence.println(input);
    }
    public void write_to_wordSentence(String input){
        word_sentence.println(input);
    }
    public void write_to_bigramSentence(String input){
        bigram_sentence.println(input);
    }
    public void write_to_trigramSentence(String input){
        trigram_sentence.println(input);
    }
    public void write_to_article(String input){
        article.println(input);
    }

    public void read_word_sentence() throws IOException{
        //iterate over 100000
        //read from word_sentence
        //update max word and max sentence
        //write to word_sentence
        String line;
        String arr[];
        int wordPos=0;
        int sentLength=0;
        for(int i=0;i<100000;i++){
            line = sentence_wordReader.readLine();
            arr = line.split(",");
            write_to_wordSentence(line);
            wordPos = Integer.parseInt(arr[2]);
            String sent = sentenceMap.get(arr[0]);
            if(sent!=null){
                write_to_sentence(arr[0]+sent);
                sentenceMap.remove(arr[0]);
                String [] arr2 = (arr[0]+sent).split(",");
                sentLength = Integer.parseInt(arr2[1]);
                String article = articleMap.get(arr2[2]);
                if(article!=null){
                    write_to_article(arr2[2]+article);
                    articleMap.remove(arr2[2]);
                }
            }
            List<String> bigsent = bigram_sents_map.get(arr[0]);
            if(bigsent!=null){
                for(int j=0;j<bigsent.size();j++){
                    write_to_bigramSentence(bigsent.get(j));
                    String s = bigsent.get(j);
                    String bigId = s.split(",")[1];
                    String b = bigramMap.get(bigId);
                    if(b!= null){
                        write_bigram(bigId+b);
                    }
                }
                bigram_sents_map.remove(arr[0]);
            }


            List<String> trigsent = trigram_sents_map.get(arr[0]);
            if(trigsent!=null){
                for(int j=0;j<trigsent.size();j++){
                    write_to_trigramSentence(trigsent.get(j));
                    String s = trigsent.get(j);
                    String trigId = s.split(",")[1];
                    String t = trigramMap.get(trigId);
                    if(t!= null){
                        write_trigram(trigId + t);
                    }
                }
                trigram_sents_map.remove(arr[0]);
            }

            String word = wordMap.get(arr[1]);
            if(word!=null){
                write_word(arr[1]+word);
                wordMap.remove(arr[1]);
            }
            if(i==99999){
                i=i-(sentLength-wordPos);
            }
        }
        //System.out.println(maxSentence+"/t"+maxWord);
    }

    public void create_folder(int file) throws IOException{
        //create folder
        //create writers
        File dir = new File("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file);
        dir.mkdir();

        if(bigram_sentence!=null){
            bigram_sentence.flush();
            bigram_sentence.close();
        }
        bigram_sentence = new PrintWriter(new BufferedWriter(new FileWriter("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file+"/sentence_bigram.csv", true)));
        bigram_sentence.println("\"SENTENCE_ID\",\"BIGRAM_ID\",\"POSITION\"");

        if(trigram_sentence!=null){
            trigram_sentence.flush();
            trigram_sentence.close();
        }
        trigram_sentence = new PrintWriter(new BufferedWriter(new FileWriter("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file+"/sentence_trigram.csv", true)));
        trigram_sentence.println("\"SENTENCE_ID\",\"TRIGRAM_ID\",\"POSITION\"");
        if(word!=null){
            word.flush();
            word.close();
        }

        word = new PrintWriter(new BufferedWriter(new FileWriter("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file+"/word.csv", true)));
        word.println("\"ID\",\"VAL\",\"FREQUENCY\"");
        if(sentence!=null){
            sentence.flush();
            sentence.close();
        }
        sentence = new PrintWriter(new BufferedWriter(new FileWriter("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file+"/sentence.csv", true)));
        sentence.println("\"ID\",\"WORDS\",\"ARTICLE_ID\",\"POSITION\"");
        if(word_sentence!=null){
            word_sentence.flush();
            word_sentence.close();
        }
        word_sentence = new PrintWriter(new BufferedWriter(new FileWriter("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file+"/word_sentence.csv", true)));
        word_sentence.println("\"SENTENCE_ID\",\"WORD_ID\",\"POSITION\"");
        if(article!=null){
            article.flush();
            article.close();
        }
        article = new PrintWriter(new BufferedWriter(new FileWriter("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file+"/article.csv", true)));
        article.println("\"ID\",\"TOPIC\",\"AUTHOR\",\"CATEGORY\",\"SUBCAT1\",\"YEAR\",\"MONTH\",\"DAY\"");

        if(bigram!=null){
            bigram.flush();
            bigram.close();
        }
        bigram = new PrintWriter(new BufferedWriter(new FileWriter("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file+"/bigram.csv", true)));
        bigram.println("\"ID\",\"WORD1\",\"WORD2\",\"FREQUENCY\"");

        if(trigram!=null){
            trigram.flush();
            trigram.close();
        }
        trigram = new PrintWriter(new BufferedWriter(new FileWriter("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/normalized/"+file+"/trigram.csv", true)));
        trigram.println("\"ID\",\"WORD1\",\"WORD2\",\"WORD3\",\"FREQUENCY\"");
    }
    public void control() throws Exception{
        wordReader = new BufferedReader(new FileReader("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/50mil/word.csv"));
        wordReader.readLine();
        sentenceReader = new BufferedReader(new FileReader("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/50mil/sentence.csv"));
        sentenceReader.readLine();
        sentence_wordReader = new BufferedReader(new FileReader("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/50mil/sentence_word.csv"));
        sentence_wordReader.readLine();
        articleReader = new BufferedReader(new FileReader("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/50mil/article.csv"));
        articleReader.readLine();
        bigramSentReader = new BufferedReader(new FileReader("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/50mil/sentence_bigram.csv"));
        bigramSentReader.readLine();
        bigramReader = new BufferedReader(new FileReader("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/50mil/bigram.csv"));
        bigramReader.readLine();
        trigramSentReader = new BufferedReader(new FileReader("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/50mil/sentence_trigram.csv"));
        trigramSentReader.readLine();
        trigramReader = new BufferedReader(new FileReader("/Users/dimuthuupeksha/Documents/Academic/FYP/CSV/50mil/trigram.csv"));
        trigramReader.readLine();



        String line;
        String temp[];
        System.out.println("1");
        while((line=wordReader.readLine())!=null){
            temp = line.split(",");
            String id = temp[0];
            wordMap.put(id,line.substring(id.length()));
        }
        System.out.println("2");
        while((line=sentenceReader.readLine())!=null){
            temp = line.split(",");
            String id = temp[0];
            sentenceMap.put(id,line.substring(id.length()));
        }
        System.out.println("3");
        while((line=articleReader.readLine())!=null){
            temp = line.split(",");
            String id = temp[0];
            articleMap.put(id,line.substring(id.length()));
        }
        System.out.println("4");
        while((line=bigramSentReader.readLine())!=null){
            temp = line.split(",");
            String id = temp[0];
            List<String> lst = bigram_sents_map.get(id);
            if(lst==null){
                lst=new ArrayList<>();
            }
            lst.add(line);
            bigram_sents_map.put(id,lst);
        }
        System.out.println("5");
        while((line=bigramReader.readLine())!=null){
            temp = line.split(",");
            String id = temp[0];
            bigramMap.put(id,line.substring(id.length()));
        }
        System.out.println("6");
        while((line=trigramSentReader.readLine())!=null){
            temp = line.split(",");
            String id = temp[0];
            List<String> lst = trigram_sents_map.get(id);
            if(lst==null){
                lst=new ArrayList<>();
            }
            lst.add(line);
            trigram_sents_map.put(id,lst);
        }
        System.out.println("7");
        while((line=trigramReader.readLine())!=null){
            temp = line.split(",");
            String id = temp[0];
            trigramMap.put(id,line.substring(id.length()));
        }
        System.out.println("8");
        for(int i=0;i<55;i++){
            create_folder(i);
            read_word_sentence();
        }
    }

    public CSV_Creator(){
        try{
            control();
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    public static void main(String as[]){
        new CSV_Creator();
    }


}
