package cs5200.edu.neu.cs5200.jga.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;

import cs5200.edu.neu.cs5200.jga.jdbc.Table;
import cs5200.edu.neu.cs5200.jga.jdbc.Column;

public class DbDao {
	private Connection connection;
	private Statement statement;
	private static DbDao dbManager = null;
	

	private DbDao(Connection connection) {
		this.connection = connection;
	}

	public DbDao() {
		
	}

	public static DbDao newInstance() {
		if(dbManager == null)
			dbManager = new DbDao();
		return dbManager;
	}
	
	public static DbDao newInstance(Connection con) {
		if(dbManager == null)
			dbManager = new DbDao(con);
		return dbManager;
	}
	
	public int deleteRecord(String tableName, int id) {
		try {
			String sql = "DELETE FROM " + tableName + " WHERE ID=" + id;
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);
			statement.close();
			return result;
		} catch(Exception e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	public int insertRecord(String tableName, Hashtable<String, String> fields) {
		try {
			String cols = "(";
			String values = "(";
			Enumeration<String> keys = fields.keys();
			while(keys.hasMoreElements()) {
				String key = keys.nextElement();
				String value = fields.get(key);
				cols += key + ", ";
				values += "'" + value + "', ";
			}
			cols = cols.substring(0, cols.length()-2);
			values = values.substring(0, values.length()-2);
			cols += ")";
			values += ")";
			String sql = "INSERT INTO " + tableName + " " + cols + " VALUES " + values + ";";
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);
			statement.close();
			return result;
		} catch(Exception e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	public ResultSet selectById(String tableName, int id) {
		try {
			String sql = "SELECT * FROM " + tableName + " WHERE ID=" + id;
			statement = getConnection().createStatement();
			ResultSet result = statement.executeQuery(sql);
			return result;
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public ResultSet selectAll(String tableName) {
		try {
			String sql = "SELECT * FROM " + tableName;
			statement = getConnection().createStatement();
			ResultSet result = statement.executeQuery(sql);
			return result;
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public int dropColumn(String tableName, String columnName) {
		try {
			String sql = "ALTER TABLE " + tableName + " DROP COLUMN " + columnName;
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);
			statement.close();
			return result;
		} catch(Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public int modifyColumnType(String tableName, Column newColumn) {
		try {
			String sql = "ALTER TABLE " + tableName + " MODIFY " + newColumn.toSql();
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);
			statement.close();
			return result;
		} catch(Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public int renameColumn(String tableName, String oldColumnName, String newColumnName) {
		try {
			Table table = getTable(tableName);
			ListIterator<Column> iterator = table.getColumns().listIterator();
			Column oldColumn = null;
			while(iterator.hasNext()) {
				oldColumn = iterator.next();
				if(oldColumn.getName().equals(oldColumnName))
					break; 
			}
			oldColumn.setName(newColumnName);
			String sql = "ALTER TABLE " + tableName + " CHANGE " + oldColumnName + " " + oldColumn.toSql();
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);;
			statement.close();
			return result;
		} catch(Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public int addColumn(String tableName, Column newColumn) {
		try {
			String sql = "ALTER TABLE " + tableName + " ADD " + newColumn.toSql();
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);
			statement.close();
			return result;
		} catch(Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public int renameTable(String oldName, String newName) {
		try {
			String sql = "ALTER TABLE " + oldName + " RENAME TO " + newName;
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);
			statement.close();
			return result;
		} catch(Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	public int createTable(Table table) {
		try {
			String sql = table.toSql();
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);
			statement.close();
			return result;
		} catch(Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public int dropTable(String tableName) {
		try {
			String sql = "DROP TABLE " + tableName + ";";
			statement = getConnection().createStatement();
			int result = statement.executeUpdate(sql);
			statement.close();
			return result;
		} catch(Exception e) {
			return 0;
		}
	}
	
	public List<Table> getAllTables() {
		List<Table> tables = null;
		try {
			DatabaseMetaData metaData = getConnection().getMetaData();
			ResultSet tablesMeta = metaData.getTables(null, null, null, null);
			tables = new ArrayList<Table>();
			while(tablesMeta.next()) {
				String tableName = tablesMeta.getString("TABLE_NAME");
				Table table = getTable(tableName);
				tables.add(table);
			}
			tablesMeta.close();
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
		return tables;
	}
	
	public Table getTable(String tableName) {
		Table table = null;
		try {
			table = new Table(tableName, getColumns(tableName));
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
		return table;
	}
	
	public List<Column> getColumns(String tableName) {
		List<Column> columnList = null;
		try {
			columnList = new ArrayList<Column>();
			DatabaseMetaData metaData = getConnection().getMetaData();
			ResultSet columns = metaData.getColumns(null, null, tableName, null);
			while(columns.next()) {
				String name = columns.getString("COLUMN_NAME");
				int type = columns.getInt("DATA_TYPE");
				int size = columns.getInt("COLUMN_SIZE");
				Column column = new Column(name, type, size);
				columnList.add(column);
			}
			columns.close();
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
		return columnList;
	}
	
	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	
	public void closeConnection() {
		try {
			connection.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void printAllTableNames(List<Table> tables) {
		ListIterator<Table> iterator = tables.listIterator();
		while(iterator.hasNext()) {
			Table table = iterator.next();
			System.out.println("   "+table.getName());
		}		
	}

	public static void main(String[] args) {
		Connection con = null;
		Table table321 = null;
		DbDao singleton = null;
		try {
			String url = "jdbc:mysql://localhost:3306/cs5200";
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(url,"YOURUSER","YOURPASSWORD");
			singleton = DbDao.newInstance(con);
			
			System.out.println("[1] Print all tables before creating a new one");
			List<Table> allTables = singleton.getAllTables();
			singleton.printAllTableNames(allTables);
			
			System.out.println("[2] Creating a new table table321 using the API");
			Column idCol   = new Column("id",   java.sql.Types.INTEGER, 10);
			Column nameCol = new Column("name", java.sql.Types.VARCHAR, 20);
			Column[] columns = {idCol, nameCol};
			List<Column> columnList = Arrays.asList(columns);
			table321 = new Table("table321", columnList);
			singleton.createTable(table321);
			
			System.out.println("[*] All tables");
			singleton.printAllTableNames(singleton.getAllTables());

			System.out.println("[3] Renaming table321 to table123");
			String sql = "ALTER TABLE table321 RENAME TO table123;";
			Statement statement = con.createStatement();
			statement.executeUpdate(sql);
	
			System.out.println("[*] All tables");
			singleton.printAllTableNames(singleton.getAllTables());

			System.out.println("[*] Table table123");
			Table table123 = singleton.getTable("table123");
			System.out.println(table123.toSql());

			System.out.println("[*] Add sex INTEGER column");
			sql = "ALTER TABLE table123 ADD sex INTEGER(1)";
			statement.executeUpdate(sql);
			
			System.out.println("[*] Table table123");
			table123 = singleton.getTable("table123");
			System.out.println(table123.toSql());
			
			System.out.println("[*] Rename sex to gender ");
			sql = "ALTER TABLE table123 CHANGE sex gender INTEGER(1);";
			statement.executeUpdate(sql);

			System.out.println("[*] Table table123");
			table123 = singleton.getTable("table123");
			System.out.println(table123);
			
			System.out.println("[*] Change gender to VARCHAR");
			sql = "ALTER TABLE table123 MODIFY gender VARCHAR(20)";
			statement.executeUpdate(sql);

			System.out.println("[*] Table table123");
			table123 = singleton.getTable("table123");
			System.out.println(table123);
			
			System.out.println("[*] Drop gender column");
			sql = "ALTER TABLE table123 DROP COLUMN gender";
			statement.executeUpdate(sql);

			System.out.println("[*] Table table123");
			table123 = singleton.getTable("table123");
			System.out.println(table123);			
			
			System.out.println("[*] Dropping table123");
			singleton.dropTable("table123");
			
			System.out.println("[*] All tables");
			singleton.printAllTableNames(singleton.getAllTables());

		} catch(Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("[*] All tables");
		singleton.printAllTableNames(singleton.getAllTables());
		
		Column idCol   = new Column("id",   java.sql.Types.INTEGER, 10);
		Column nameCol = new Column("name", java.sql.Types.VARCHAR, 20);
		Column[] columns = {idCol, nameCol};
		List<Column> columnList = Arrays.asList(columns);
		Table tableABC = new Table("tableABC", columnList);

		System.out.println("[*] Create tableABC");
		singleton.createTable(tableABC);

		System.out.println("[*] All tables");
		singleton.printAllTableNames(singleton.getAllTables());
		
		System.out.println("[*] Rename tableABC to tableCBA");
		singleton.renameTable("tableABC", "tableCBA");
		
		System.out.println("[*] All tables");
		singleton.printAllTableNames(singleton.getAllTables());
		
		System.out.println("[*] Get tableCBA");
		Table tableCBA = singleton.getTable("tableCBA");
		
		System.out.println("[*] Get tableCBA");
		System.out.println(tableCBA);
		
		System.out.println("[*] Rename fields tableCBA.name to tableCBA.age");
		singleton.renameColumn("tableCBA", "name", "age");
		tableCBA = singleton.getTable("tableCBA");
		System.out.println(tableCBA);
		
		System.out.println("[*] Change tableCBA.age to INTEGER(10)");
		Column newColumn = new Column("age",java.sql.Types.INTEGER, 10);
		singleton.modifyColumnType("tableCBA", newColumn);
		tableCBA = singleton.getTable("tableCBA");
		System.out.println(tableCBA);
		
		System.out.println("[*] Drop tableCBA.age");
		singleton.dropColumn("tableCBA", "age");
		tableCBA = singleton.getTable("tableCBA");
		System.out.println(tableCBA);
		
		System.out.println("[*] Drop tableCBA");
		singleton.dropTable("tableCBA");
		
		System.out.println("[*] All tables");
		singleton.printAllTableNames(singleton.getAllTables());
		
		singleton.closeConnection();
	}
}
