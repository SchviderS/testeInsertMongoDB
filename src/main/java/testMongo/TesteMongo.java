package testMongo;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class TesteMongo {
	
	static MongoClient mongo = new MongoClient( "localhost" , 27017 );
	static MongoDatabase db = mongo.getDatabase("testdb");
	static MongoCollection<Document> collection = db.getCollection("testdb");
	
	static int qtd = 101;
	static int valor1 = 3;
	static int valor2 = 10;
	static int escala = 1000;
	
	public static void main(String[] args) {
		System.out.println("------- TESTE MongoDB -------");
		insert(qtd);
		select();
		update();
		where(valor1, valor2);
		delete();
//		drop();
	}		
	
	private static void insert (int qtd){
		ArrayList<Long> list = new ArrayList<Long>();
		
		int i = 0;
		while ( i < qtd ){
			
			long tempoInicio = System.nanoTime()/escala;			
			collection.insertOne(new Document("doc",i));
			long tempoFinal = System.nanoTime()/escala;
			
//			System.out.println("Tempo CADA INSERT: "+(tempoFinal - tempoInicio)+" micro segundos");
			list.add(tempoFinal - tempoInicio);
			i++;
		}
		
		long media = 0;
		for(int j = 1; j < list.size(); j++){
			media = media + list.get(j);
		}
		System.out.println("Media INSERT: "+ media/(list.size()-1)+ " microsegundos (ou "+toMili(media/(list.size()-1))+" milisegundos)");
	}
		
	private static void update(){
		ArrayList<Long> list = new ArrayList<Long>();
		
		for(Document doc : collection.find()){
			long tempoInicio = System.nanoTime()/escala;
			collection.updateOne(doc,new Document("$set", new Document("outra", "coisa")));
			long tempoFinal = System.nanoTime()/escala;
			
//			System.out.println("Tempo Total: "+(tempoFinal - tempoInicio)+" micro segundos PARA CADA UPDATE");
			list.add(tempoFinal - tempoInicio);
		}
		
		long media = 0;
		for(int j = 1; j < list.size(); j++){
			media = media + list.get(j);
		}
		System.out.println("Media UPDATE: "+ media/(list.size()-1)+ " microsegundos (ou "+toMili(media/(list.size()-1))+" milisegundos)");
	}
		
	private static void select(){
		long tempoInicio = System.nanoTime()/escala;
		FindIterable<Document> col = collection.find();
		long tempoFinal = System.nanoTime()/escala;
		System.out.println("Tempo PARA SELECT *: "+(tempoFinal - tempoInicio)+" micro segundos (ou "+toMili(tempoFinal - tempoInicio)+" milisegundos)");	
		
		for(Document doc : col){
//			System.out.println(doc.toJson());
		}
	}
	
	private static void drop(){
		long tempoInicio = System.nanoTime()/escala;
		collection.drop();
		long tempoFinal = System.nanoTime()/escala;
		System.out.println("Tempo PARA DROP DA COLECAO: "+(tempoFinal - tempoInicio)+" micro segundos (ou "+toMili(tempoFinal - tempoInicio)+" milisegundos)");
	}
	
	private static void delete(){
		FindIterable<Document> col = collection.find();
		ArrayList<Long> list = new ArrayList<Long>();
		
		for(Document doc : col){
			long tempoInicio = System.nanoTime()/escala;
			collection.deleteOne(doc);
			long tempoFinal = System.nanoTime()/escala;
			
//			System.out.println("Tempo Total: "+(tempoFinal - tempoInicio)+" micro segundos PARA CADA DELETE");
			list.add(tempoFinal - tempoInicio);
		}
		
		long media = 0;
		for(int j = 1; j < list.size(); j++){
			media = media + list.get(j);
		}
		System.out.println("Media DELETE: "+ media/(list.size()-1)+ " microsegundos (ou "+toMili(media/(list.size()-1))+" milisegundos)");
	}
	
	private static void where(int valor1, int valor2){
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("doc", new BasicDBObject("$gt", valor1).append("$lt", valor2));
		
		long tempoInicio = System.nanoTime()/escala;
		MongoCursor<Document> cursor = collection.find(gtQuery).iterator();
		long tempoFinal = System.nanoTime()/escala;
		System.out.println("Tempo PARA WHERE 'BETWEEN': "+(tempoFinal - tempoInicio)+" micro segundos (ou "+toMili(tempoFinal - tempoInicio)+" milisegundos)");
		
		int count = 0;
		while (cursor.hasNext()) {
			count ++;
			cursor.next();
//			System.out.println(cursor.next()); // Se for printar os resultados, comentar a linha do cursor.next(), para não dar erro na contagem
		}
		System.out.println(count+" resultados");
	}
	
	public static String toMili(long valor){
		DecimalFormat df = new DecimalFormat("#.#########");
		df.setRoundingMode(RoundingMode.CEILING);
		return df.format(valor/1000f);
	}
}