package testPostgre;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;

public class TestePG {

	static String user = "postgres";
	static String pass = "univel";
	static String database = "testdb";
	static String host = "localhost";
	static String porta = "5432";

	static String tabela = "testedb";
	static String col1 = "ID";
	static String col2 = "CONTEUDO_TEXT";
	static String col3 = "CONTEUDO_INT";
	static String col4 = "CONTEUDO_CHAR";
	static String col5 = "CONTEUDO_REAL";
	
	static int quantidade = 101;
	static int valor1 = 40;
	static int valor2 = 45;
	static int escala = 1000; //Microsegundos
	
	public static void main(String[] args) {
		System.out.println("------- TESTE PostgreSQL -------");
		conectar();
//		criarTabela(tabela, col1, col2, col3, col4, col5); //Comentar após criar a tabela, para não tentar criar novamente
		insert(quantidade, tabela, col1, col2, col3, col4, col5);
		select();
		update(quantidade, tabela, col5);
		where(valor1, valor2);
		delete(quantidade, tabela);
//		drop(tabela);  //Drop não funcionando via Java, só direto no banco
	}

	public static Connection conectar() {
		Connection con = null;
		try {
			Class.forName("org.postgresql.Driver");
			con = DriverManager.getConnection("jdbc:postgresql://" + host + ":"
					+ porta + "/" + database, user, pass);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		return con;

	}
	
	public static void criarTabela(String tabela, String col1, String col2, String col3, String col4, String col5) {
		
		Connection con = conectar();
		if (con != null) {
			try {
				Statement stmt = con.createStatement();
				String sql = "CREATE TABLE "+ tabela +" ( "
						+ col1 + " INT PRIMARY KEY NOT NULL,"
						+ col2 + " TEXT NOT NULL, "
						+ col3 + " INT  NOT NULL, "
						+ col4 + " CHAR(50), "
						+ col5 + " REAL)";
				
				long tempoInicio = System.nanoTime()/escala;
				stmt.executeUpdate(sql);
				long tempoFinal = System.nanoTime()/escala;
				System.out.println("Tempo de CRIAÇÃO DA TABELA: "+(tempoFinal - tempoInicio)+" micro segundos  (ou "+toMili(tempoFinal - tempoInicio)+" milisegundos");
				
				stmt.close();
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
			System.out.println("Tabela "+ tabela +" criada com sucesso!");
		}
	}
	
	private static void insert(int quantidade, String tabela, String col1, String col2, String col3, String col4, String col5) {
		Connection con = conectar();
		ArrayList<Long> list = new ArrayList<Long>();
		if (con != null) {
			try {
				con.setAutoCommit(false);
				for(int i = 1; i < quantidade; i++){
					Statement stmt = con.createStatement();
					
					String sql = "INSERT INTO " + tabela + " ("+col1+","+col2+","+col3+","+col4+","+col5+") "
							+ "VALUES ("+i+", "+"'Pessoa"+i+"', "+(i+20)+", 'Lugar"+i+"', "+(i*1000f)+" ) ";

					long tempoInicio = System.nanoTime()/escala;
					stmt.executeUpdate(sql);
					con.commit();
					long tempoFinal = System.nanoTime()/escala;
					
//					System.out.println("Tempo cada INSERT: "+(tempoFinal - tempoInicio)+" micro segundos");
					list.add(tempoFinal-tempoInicio);
					
					stmt.close();
				}

				con.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
			
			long media = 0;
			for(int j = 1; j < list.size(); j++){
				media = media + list.get(j);
			}
			System.out.println("Media INSERT: "+ media/(list.size()-1)+ " microsegundos (ou "+toMili(media/(list.size()-1))+" milisegundos)");
		}
	}

	private static void select() {
		Connection con = conectar();
		if(con != null){
			try {
				 Statement stmt = con.createStatement();		 
				 con.setAutoCommit(false);
				 
				 long tempoInicio = System.nanoTime()/escala;
		         ResultSet rs = stmt.executeQuery( "SELECT * FROM " + tabela );
		         con.commit();
		         long tempoFinal = System.nanoTime()/escala;
		         System.out.println("Tempo SELECT *: "+(tempoFinal - tempoInicio)+" micro segundos (ou "+toMili(tempoFinal - tempoInicio)+" milisegundos");
		         
		         while ( rs.next() ) {		  
		            int result1 = rs.getInt(col1);
		            String  result2 = rs.getString(col2);
		            int result3  = rs.getInt(col3);
		            String  result4 = rs.getString(col4);
		            float result5 = rs.getFloat(col5);
		            
//		            System.out.println( col1 + " = " + result1 );
//		            System.out.println( col2 + " = " + result2 );
//		            System.out.println( col3 + " = " + result3 );
//		            System.out.println( col4 + " = " + result4 );
//		            System.out.println( col5 + " = " + result5 );
		           
		         }
		         rs.close();
		         stmt.close();
		         con.close();
			} catch ( Exception e ) {
				e.printStackTrace();
	    	   	System.exit(0);
	       	}
		}
	}

	private static void update(int quantidade, String tabela, String col5){
		Connection con = conectar();
		ArrayList<Long> list = new ArrayList<Long>();
		try {
			con.setAutoCommit(false);
	        Statement stmt = con.createStatement();
	        
	        for(int i = 1; i < quantidade; i++){
	        	String sql = "UPDATE " + tabela + " set " + col5 + " = "+(i*500f)+" where ID="+i;
	        	
	        	long tempoInicio = System.nanoTime()/escala;
				stmt.executeUpdate(sql);
				con.commit();
				long tempoFinal = System.nanoTime()/escala;
				
//				System.out.println("Tempo cada UPDATE: "+(tempoFinal - tempoInicio)+" micro segundos");
				list.add(tempoFinal - tempoInicio);
	        }
	        stmt.close();
	        con.close();
        } catch ( Exception e ) {
        	e.printStackTrace();
        	System.exit(0);
        }
		
		long media = 0;
		for(int j = 1; j < list.size(); j++){
			media = media + list.get(j);
		}
		System.out.println("Media UPDATE: "+ media/(list.size()-1)+ " microsegundos (ou "+toMili(media/(list.size()-1))+" milisegundos)");
	}
	
	private static void delete(int quantidade, String tabela){
		Connection con = conectar();
		Statement stmt;
		ArrayList<Long> list = new ArrayList<Long>();
		
 		try{
			con.setAutoCommit(false);
			stmt = con.createStatement();
			for(int i = 1; i < quantidade; i++){
				String sql = "DELETE from " + tabela + " WHERE " + col1 + "=" + i;
				
				long tempoInicio = System.nanoTime()/escala;
				stmt.executeUpdate(sql);
				con.commit();
				long tempoFinal = System.nanoTime()/escala;
//				System.out.println("Tempo cada DELETE: "+(tempoFinal - tempoInicio)+" micro segundos");
				list.add(tempoFinal - tempoInicio);
				
			}
			stmt.close();
			con.close(); 
      	} catch ( Exception e ) {
	        e.printStackTrace();
	        System.exit(0);
      	}
		
		long media = 0;
		for(int j = 1; j < list.size(); j++){
			media = media + list.get(j);
		}
		System.out.println("Media DELETE: "+ media/(list.size()-1)+ " microsegundos (ou "+toMili(media/(list.size()-1))+" milisegundos)");
	}
	
	private static void where(int valor1, int valor2){
		Connection con = conectar();
		if(con != null){
			try {
				 Statement stmt = con.createStatement();		 
				 con.setAutoCommit(false);
				 
				 long tempoInicio = System.nanoTime()/escala;
		         ResultSet rs = stmt.executeQuery( "SELECT * FROM " + tabela + " WHERE "+ col3 + " BETWEEN " + valor1 + " AND " + valor2);
		         con.commit();
		         long tempoFinal = System.nanoTime()/escala;
		         System.out.println("Tempo execução SELECT COM WHERE BETWEEN: "+(tempoFinal - tempoInicio)+" micro segundos (ou "+toMili(tempoFinal - tempoInicio)+" milisegundos");
		         
		         if(rs != null){
		        	 int count = 0;
			         while ( rs.next() ) {
			        	count ++;
			            int result1 = rs.getInt(col1);
			            String  result2 = rs.getString(col2);
			            int result3  = rs.getInt(col3);
			            String  result4 = rs.getString(col4);
			            float result5 = rs.getFloat(col5);   
//			            System.out.println( col1 + " = " + result1 );
//			            System.out.println( col2 + " = " + result2 );
//			            System.out.println( col3 + " = " + result3 );
//			            System.out.println( col4 + " = " + result4 );
//			            System.out.println( col5 + " = " + result5 );
			         }
			         System.out.println(count + " resultados");
		         }else
		        	 System.out.println("Resultset vazio");
		         
		         rs.close();
		         stmt.close();
		         con.close();
			} catch ( Exception e ) {
				e.printStackTrace();
	    	   	System.exit(0);
	       	}
		}
	}
	
	private static void drop(String tabela){
		Connection con = conectar();
		if(con != null){
			try {
				 Statement stmt = con.createStatement();		 
				 con.setAutoCommit(false);
				 
				 long tempoInicio = System.nanoTime()/escala;
				 stmt.executeUpdate("DROP TABLE "+ tabela);
				 con.commit();
				 long tempoFinal = System.nanoTime()/escala;
		         System.out.println("Tempo execução DROP: "+(tempoFinal - tempoInicio)+" micro segundos  (ou "+toMili(tempoFinal - tempoInicio)+" milisegundos");
				 
				 stmt.close();
		         con.close();
			} catch ( Exception e ) {
				e.printStackTrace();
	    	   	System.exit(0);
	       	}
		}
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static String toMili(long valor){
		DecimalFormat df = new DecimalFormat("#.#########");
		df.setRoundingMode(RoundingMode.CEILING);
		return df.format(valor/1000f);
	}
}
