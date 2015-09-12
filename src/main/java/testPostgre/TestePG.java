package testPostgre;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

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
	static int quantidade = 100;
	
	public static void main(String[] args) {
		conectar();
		criarTabela(tabela, col1, col2, col3, col4, col5);
		insert(quantidade, tabela, col1, col2, col3, col4, col5);
		select();
		update(quantidade, tabela, col5);
		delete(quantidade, tabela);
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
				
				long tempoInicio = System.nanoTime()/1000;
				stmt.executeUpdate(sql);
				System.out.println("Tempo de CRIAÇÃO DA TABELA: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos");
				
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

		if (con != null) {

			try {
				con.setAutoCommit(false);

				for(int i = 1; i < quantidade; i++){
					Statement stmt = con.createStatement();
					
					String sql = "INSERT INTO " + tabela + " ("+col1+","+col2+","+col3+","+col4+","+col5+") "
							+ "VALUES ("+i+", +'Pessoa"+i+"', "+(i+20)+", 'Lugar"+i+"', "+(i*1000f)+" ) ";

					long tempoInicio = System.nanoTime()/1000;
					stmt.executeUpdate(sql);
					System.out.println("Tempo cada INSERT: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos");  
					
					stmt.close();
					con.commit();
				}

				con.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
			System.out.println("Registros criados com sucesso!");
		}
	}

	private static void select() {
		Connection con = conectar();
		if(con != null){
			try {
				 Statement stmt = con.createStatement();		 
				 con.setAutoCommit(false);
		         ResultSet rs = stmt.executeQuery( "SELECT * FROM " + tabela );
		         while ( rs.next() ) {
		        	 
		        	long tempoInicio = System.nanoTime()/1000;
		            int result1 = rs.getInt(col1);
		            String  result2 = rs.getString(col2);
		            int result3  = rs.getInt(col3);
		            String  result4 = rs.getString(col4);
		            float result5 = rs.getFloat(col5);
		            System.out.println("Tempo cada SELECT: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos");
		            
		            System.out.println( col1 + " = " + result1 );
		            System.out.println( col2 + " = " + result2 );
		            System.out.println( col3 + " = " + result3 );
		            System.out.println( col4 + " = " + result4 );
		            System.out.println( col5 + " = " + result5 );
		           
		            
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
		try {
			con.setAutoCommit(false);
	        Statement stmt = con.createStatement();
	        
	        for(int i = 1; i < quantidade; i++){
	        	String sql = "UPDATE " + tabela + " set " + col5 + " = "+(i*500f)+" where ID="+i;
	        	
	        	long tempoInicio = System.nanoTime()/1000;
				stmt.executeUpdate(sql);
				con.commit();
				System.out.println("Tempo cada UPDATE: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos");
	            
	        }
	        stmt.close();
	        con.close();
        } catch ( Exception e ) {
        e.printStackTrace();
        System.exit(0);
        }
	}
	
	private static void delete(int quantidade, String tabela){
		Connection con = conectar();
		Statement stmt;
		try{
			con.setAutoCommit(false);
			stmt = con.createStatement();
			for(int i = 1; i < quantidade; i++){
				String sql = "DELETE from " + tabela + " where ID="+i;
				
				long tempoInicio = System.nanoTime()/1000;
				stmt.executeUpdate(sql);
				con.commit();
				System.out.println("Tempo cada DELETE: "+(System.nanoTime()/1000-tempoInicio)+" micro segundos");
			}
			stmt.close();
			con.close(); 
      	} catch ( Exception e ) {
	        e.printStackTrace();
	        System.exit(0);
      	}
	}
}
