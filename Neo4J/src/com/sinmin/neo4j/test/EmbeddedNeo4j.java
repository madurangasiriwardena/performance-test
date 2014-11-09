package com.sinmin.neo4j.test;

import java.io.File;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class EmbeddedNeo4j {
    private static final String DB_PATH = "/Users/dimuthuupeksha/Documents/Academic/FYP/neo4j/data/graph.db";

    public String greeting;

    // START SNIPPET: vars
    GraphDatabaseService graphDb;
    Node firstNode;
    Node secondNode;
    Relationship relationship;
    // END SNIPPET: vars

    // START SNIPPET: createReltype
    private static enum RelTypes implements RelationshipType {
        KNOWS
    }
    // END SNIPPET: createReltype

    /*public static void main( final String[] args )
    {
        EmbeddedNeo4j hello = new EmbeddedNeo4j();
        hello.createDb();
        //hello.removeData();
        //hello.shutDown();
    }*/

    void createDb() {
        deleteFileOrDirectory(new File(DB_PATH));
        // START SNIPPET: startDb
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        registerShutdownHook(graphDb);
        // END SNIPPET: startDb

        // START SNIPPET: transaction
        ExecutionEngine engine = new ExecutionEngine(graphDb);
        ExecutionResult result;
        try (Transaction tx = graphDb.beginTx()) {
            //result = engine.execute("create (n:Word) return n");
            /*// Database operations go here
            // END SNIPPET: transaction
            // START SNIPPET: addData
            firstNode = graphDb.createNode();
            firstNode.setProperty( "message", "Hello, " );
            secondNode = graphDb.createNode();
            secondNode.setProperty( "message", "World!" );

            relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
            relationship.setProperty( "message", "brave Neo4j " );
            // END SNIPPET: addData

            // START SNIPPET: readData
            System.out.print( firstNode.getProperty( "message" ) );
            System.out.print( relationship.getProperty( "message" ) );
            System.out.print( secondNode.getProperty( "message" ) );
            // END SNIPPET: readData

            greeting = ( (String) firstNode.getProperty( "message" ) )
                    + ( (String) relationship.getProperty( "message" ) )
                    + ( (String) secondNode.getProperty( "message" ) );

            // START SNIPPET: transaction
            tx.success();*/
            tx.success();
        }catch(Exception ex){
            ex.printStackTrace();
        }
        // END SNIPPET: transaction
    }

    void removeData() {
        try (Transaction tx = graphDb.beginTx()) {
            // START SNIPPET: removingData
            // let's remove the data
            firstNode.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING).delete();
            firstNode.delete();
            secondNode.delete();
            // END SNIPPET: removingData

            tx.success();
        }
    }

    void shutDown() {
        System.out.println();
        System.out.println("Shutting down database ...");
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }

    // START SNIPPET: shutdownHook
    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
    // END SNIPPET: shutdownHook

    private static void deleteFileOrDirectory(File file) {
        System.out.println("Deleting");
        if (file.exists()) {
            System.out.println("Deleting 1");
            if (file.isDirectory()) {
                System.out.println("Deleting 2");
                for (File child : file.listFiles()) {
                    System.out.println("Deleting 3");
                    deleteFileOrDirectory(child);
                }
            }
            file.delete();
        }
    }
}
