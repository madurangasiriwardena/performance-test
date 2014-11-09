package com.sinmin.neo4j.oracle;

import com.sinmin.neo4j.oracle.bean.ArticleBean;

import javax.xml.transform.Result;
import java.sql.*;
import java.util.Properties;

import oracle.jdbc.*;

/**
 * Created by dimuthuupeksha on 8/29/14.
 */
public class OracleJavaClient {

    private static final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
    //private static final String DB_CONNECTION = "jdbc:oracle:thin:@//localhost:1521/PDB1";
    //private static final String DB_USER = "sinmin";
    //private static final String DB_PASSWORD = "sinmin";

    private static final String DB_CONNECTION = "jdbc:oracle:thin:@//192.248.15.239:1522/corpus.sinmin.com";
    private static final String DB_USER = "SYSTEM";
    private static final String DB_PASSWORD = "Sinmin1234";
    final String GENERATED_COLUMNS[] = {"id"};


    private static Connection getDBConnection() {
        Connection dbConnection = null;
        //try {
        //  Class.forName(DB_DRIVER);
        //} catch (ClassNotFoundException e) {
        //  System.out.println(e.getMessage());
        //}
        try {
            dbConnection = DriverManager.getConnection(
                    DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;
    }

    OracleConnection connect() throws SQLException {
        OracleDriver dr = new OracleDriver();
        Properties prop = new Properties();
        prop.setProperty("user", "sinmin");
        prop.setProperty("password", "sinmin");
        return (OracleConnection) dr.connect(DB_CONNECTION, prop);
    }

    public long addArticleToOracle(ArticleBean bean) throws SQLException {
        Connection dbConnection = null;
        Statement cs = null;
        Statement currvalSt = null;
        ResultSet currvalRst = null;
        String insertTableSQL = "INSERT INTO ARTICLE (TOPIC,AUTHOR,CATEGORY,SUBCAT1,YEAR,MONTH,DAY) values (" +
                "'" + bean.topic + "'," + "'" + bean.author + "'," + "'" + bean.category + "'," + "'" + bean.subCat1 + "'," + "" + bean.year + "," + "" + bean.month + "," + "" + bean.day + ")";
        String sql_currval = "select TOPIC_ID_INC.currval from dual";

        try {
            dbConnection = getDBConnection();

            //System.out.println(insertTableSQL);
            dbConnection.setAutoCommit(false);
            cs = dbConnection.createStatement();
            cs.executeUpdate(insertTableSQL);
            currvalSt = dbConnection.createStatement();
            currvalRst = currvalSt.executeQuery(sql_currval);
            long newId = 0;
            if (currvalRst.next()) {
                newId = currvalRst.getLong(1);
            }
            dbConnection.commit();
            return newId;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (cs != null) {
                cs.close();
            }
            if (dbConnection != null) {
                dbConnection.close();
            }
        }
        return 0;
    }

    public long addSentenceToOracle(String[] words, long articleId, int position) throws SQLException {
        long sentenceId = addSentence(words.length, articleId, position);
        if (sentenceId != 0) {
            for (int i = 0; i < words.length; i++) {

                //long wordId = addWord(words[i]);
                //if(wordId>0){
                //  addWordSentence(sentenceId,wordId,i+1);
                //}
            }
        }
        return 0;
    }

    private void addWordSentence(long sentenceId, long wordId, int position) throws SQLException {
        String addWordSentenceSQL = "INSERT INTO SENTENCE_WORD (SENTENCE_ID,WORD_ID,POSITION) values (" + sentenceId + "," + wordId + "," + position + ")";
        Connection dbConnection = null;
        Statement statement = null;

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();
            statement.executeUpdate(addWordSentenceSQL);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {

            if (statement != null) {
                statement.close();
            }
            if (dbConnection != null) {
                dbConnection.close();
            }
        }
    }

    private long addWord(String word) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;
        Statement cs = null;
        Statement currvalSt = null;
        ResultSet currvalRst = null;
        String insertTableSQL = "INSERT INTO WORD (VAL,FREQUENCY) values ('" + word + "',1)";
        String searchSQL = "SELECT * FROM WORD WHERE VAL='" + word + "'";
        String sql_currval = "select WORD_ID_INC.currval from dual";
        try {
            dbConnection = getDBConnection();

            //System.out.println(insertTableSQL);
            statement = dbConnection.createStatement();
            ResultSet rst = statement.executeQuery(searchSQL);
            if (rst.next()) {
                dbConnection.setAutoCommit(true);
                int frequency = rst.getInt(3);
                long wordId = rst.getLong(1);
                String vals = rst.getString(2);
                String updateQuery = "UPDATE WORD SET FREQUENCY = " + (frequency + 1) + " WHERE ID = " + wordId;
                statement.executeUpdate(updateQuery);
                return wordId;
            } else {
                dbConnection.setAutoCommit(false);
                cs = dbConnection.createStatement();
                cs.executeUpdate(insertTableSQL);
                currvalSt = dbConnection.createStatement();
                currvalRst = currvalSt.executeQuery(sql_currval);
                long newId = 0;
                if (currvalRst.next()) {
                    newId = currvalRst.getLong(1);
                }
                dbConnection.commit();
                return newId;
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (cs != null) {
                cs.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (dbConnection != null) {
                dbConnection.close();
            }
        }
        return 0;
    }

    private long addSentence(int count, long articleId, int position) throws SQLException {
        Connection dbConnection = null;
        Statement cs = null;
        Statement currvalSt = null;
        ResultSet currvalRst = null;
        String insertTableSQL = "INSERT INTO SENTENCE (WORDS,ARTICLE_ID,POSITION) values (" + count + "," + articleId + "," + position + ")";
        String sql_currval = "select SENTENCE_ID_INC.currval from dual";
        try {
            dbConnection = getDBConnection();
            dbConnection.setAutoCommit(false);
            System.out.println(insertTableSQL);

            cs = dbConnection.createStatement();
            cs.executeUpdate(insertTableSQL);
            currvalSt = dbConnection.createStatement();
            currvalRst = currvalSt.executeQuery(sql_currval);
            long newId = 0;
            if (currvalRst.next()) {
                newId = currvalRst.getLong(1);
            }
            dbConnection.commit();
            return newId;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (cs != null) {
                cs.close();
            }
            if (dbConnection != null) {
                dbConnection.close();
            }
        }
        return 0;
    }

    public long getAddArticleTime() {
        return 0;
    }

    public long getAddSentenceTime() {
        return 0;
    }
}
