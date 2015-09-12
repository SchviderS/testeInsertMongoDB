package testMongo;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TesteMongo {
	
	public static void main(String[] args) {
		
		MongoClient mongo = new MongoClient( "localhost" , 27017 );
		
		MongoDatabase db = mongo.getDatabase("testdb");
		
		MongoCollection<Document> collection = db.getCollection("testdb");
		
//		System.out.println("Nano(ns) = 10 ^ -9; Micro(us) = 10 ^ -6; Mili(ms) = 10 ^ -3");
//		int i = 0;
//		while ( i < 200 ){
//			
//			long tempoInicio = System.nanoTime()/1000;
//			
//			collection.insertOne(new Document("doc",i));
//			
//			System.out.println("Tempo Total: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos");  
//			
//			i++;
//		}
		
		for(Document doc : collection.find()){
			long tempoInicio = System.nanoTime()/1000;
			collection.updateOne(doc,new Document("$set", new Document("outra", "coisa")));
			System.out.println("Tempo Total: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos PARA CADA UPDATE");
		}
//		
//		for(Document doc : collection.find()){
//			long tempoInicio = System.nanoTime()/1000;
//			System.out.println(doc.toJson());
//			System.out.println("Tempo Total: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos PARA CADA BUSCA");
//		}
//		
		
//		long tempoInicio = System.nanoTime()/1000000;
//		collection.drop();
//		System.out.println("Tempo Total: "+(System.nanoTime()/1000000-tempoInicio)+" mili segundos PARA DROP DA COLECAO");
	}
	
}