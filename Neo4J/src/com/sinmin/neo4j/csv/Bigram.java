package com.sinmin.neo4j.csv;

/**
 * Created by dimuthuupeksha on 11/5/14.
 */
public class Bigram {
    String id1;
    String id2;
    public Bigram(String id1,String id2){
        this.id1= id1;
        this.id2 = id2;
    }

    @Override
    public int hashCode() {
        return id1.hashCode()+id2.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        Bigram bi = (Bigram)obj;
        if((bi.id1.equals(id1)&&bi.id2.equals(id2))||(bi.id1.equals(id2)&&bi.id2.equals(id1))){
            return true;
        }else{
            return false;
        }
    }
}
