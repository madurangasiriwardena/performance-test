package com.sinmin.neo4j.rest;


import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by dimuthuupeksha on 8/22/14.
 */
public class GraphBatchInserter {

    public GraphBatchInserter() {
        BatchInserter inserter = BatchInserters.inserter("/Users/dimuthuupeksha/Documents/Academic/FYP/neo4j/data/graph.db");
        Label personLabel = DynamicLabel.label("Person");
        inserter.createDeferredSchemaIndex(personLabel).on("name").create();
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "Mattias");
        long mattiasNode = inserter.createNode(properties, personLabel);
        properties.put("name", "Chris");
        long chrisNode = inserter.createNode(properties, personLabel);
        RelationshipType knows = DynamicRelationshipType.withName("KNOWS");
// To set properties on the relationship, use a properties map
// instead of null as the last parameter.
        inserter.createRelationship(mattiasNode, chrisNode, knows, null);
        inserter.shutdown();
    }

    public Long addSentenceToGraph(String words[]) {
        return 0l;
    }

    /*public static void main(String a[]){
        new GraphBatchInserter();
    }*/
}
