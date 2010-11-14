package com.jayway.mongodb;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jayway.mongodb.AnalyzedDBObject.Condition;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class MongoAnalysisTest {

	
	static final String TEXT = "I Would like to use mongodb for full text search";
	static final String COLLECTION_NAME = "article";
	static final String INDEXED_FIELD = "indtext";
	static final String TEXT_FIELD = "text";
	static MongoDatastore mongo;
	
	static Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
	
	@BeforeClass
	public static void setup() throws UnknownHostException, MongoException {
		mongo = new MongoDatastore();
		mongo.dropDatabase();
	}
	
	@Test
	public void verifySaveAndGetTestSearchWithAll() throws IOException {

		mongo.save(
				new AnalyzedDBObject(analyzer)
				.appendAndAnalyzeFullText(INDEXED_FIELD, TEXT)
				.append(TEXT_FIELD, TEXT),
				COLLECTION_NAME);
		
		DBObject result = mongo.get(new AnalyzedDBObject(analyzer).createQuery(INDEXED_FIELD,TEXT),COLLECTION_NAME);
		assertEquals(TEXT, result.get(TEXT_FIELD));
		
		result = mongo.get(new AnalyzedDBObject(analyzer).createQuery(INDEXED_FIELD,"MonGoDB sEarch woulD TO"),COLLECTION_NAME);
		assertEquals(TEXT, result.get(TEXT_FIELD));
		
		// In this query, only "search" matches
		result = mongo.get(new AnalyzedDBObject(analyzer).createQuery(INDEXED_FIELD,"MonGoDBs wouldd search "),COLLECTION_NAME);
		assertNull(result);
	}
	
	@Test
	public void verifySaveAndGetTestSearchWithAtLeastOne() throws IOException {

		mongo.save(
				new AnalyzedDBObject(analyzer)
				.appendAndAnalyzeFullText(INDEXED_FIELD, TEXT)
				.append(TEXT_FIELD, TEXT),
				COLLECTION_NAME);
		
		DBObject result = mongo.get(new AnalyzedDBObject(analyzer).createQuery(INDEXED_FIELD,TEXT,Condition.IN),COLLECTION_NAME);
		assertEquals(TEXT, result.get(TEXT_FIELD));
		
		result = mongo.get(new AnalyzedDBObject(analyzer).createQuery(INDEXED_FIELD,"MonGoDB sEarch woulD",Condition.IN),COLLECTION_NAME);
		assertEquals(TEXT, result.get(TEXT_FIELD));
		
		// In this query, only "search" matches
		result = mongo.get(new AnalyzedDBObject(analyzer).createQuery(INDEXED_FIELD,"MonGoDBs wouldd search ",Condition.IN),COLLECTION_NAME);
		assertEquals(TEXT, result.get(TEXT_FIELD));
	}
}
