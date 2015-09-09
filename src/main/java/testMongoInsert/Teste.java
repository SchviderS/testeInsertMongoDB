package testMongoInsert;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Teste {
	
	public static void main(String[] args) {
		
		MongoClient mongo = new MongoClient( "localhost" , 27017 );
		
		MongoDatabase db = mongo.getDatabase("testdb");
		
		MongoCollection<Document> collection = db.getCollection("testdb");
		
		System.out.println("Nano(ns) = 10 ^ -9; Micro(us) = 10 ^ -6; Mili(ms) = 10 ^ -3");
		int i = 0;
		while ( i < 200 ){
			
			long tempoInicio = System.nanoTime()/1000;
			
			collection.insertOne(new Document("doc",i));
			
			System.out.println("Tempo Total: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos");  
			
			i++;
			
		}
	}
}