package com.sinmin.neo4j.beans;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dimuthuupeksha on 6/24/14.
 */
public class DivainaArticleBean {
    public int id;
    public String day;
    public String month;
    public String year;
    public String topic;
    public String author;
    public List<Long> sentenceIds = new ArrayList<>();
    public List<SentenceBean> sentences = new ArrayList<>();
}
