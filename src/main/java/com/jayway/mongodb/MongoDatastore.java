package com.jayway.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDatastore {

	DB db;
	
	public MongoDatastore() throws UnknownHostException, MongoException {
		Mongo m = new Mongo( "localhost" , 27017 );
		db = m.getDB( "mongotextsearch" );
	}
	
	private DBCollection getCollection(String name) {
		return db.getCollection(name);
	}
	
	public void dropDatabase() {
		db.dropDatabase();
	}
	
	public void ensureIndex(String col, String field) {
		getCollection(col).createIndex(new BasicDBObject(field, new Integer(1)));
	}
	
	public void save(DBObject object,String col) {
		getCollection(col).save(object);
	}
	
	public DBObject get(DBObject query,String col) {
		return getCollection(col).findOne(query);
	}
	
	public List<DBObject> find(DBObject query,String col) {
		
		DBCursor cursor = getCollection(col).find(query);
		
		List<DBObject> dbObjects = new ArrayList<DBObject>();
		while(cursor.hasNext()) {
			dbObjects.add(cursor.next());
		}
		return dbObjects;
	}
	
	public long cnt(String col) {
		return getCollection(col).count();
	}
	
}
