package cs5200.edu.neu.cs5200.jga.jdbc;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

public class Table {
	private String name;
	private List<Column> columns;

	public Table(String name) {
		this.name = name;
		Column id = new Column("id", Types.INTEGER, 10);
		Column[] cols = {id};
		this.columns = Arrays.asList(cols);
	}
	public Table(String name, List<Column> columns) {
		this(name);
		this.columns = columns;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	
	public String toString() {
		String tableStr = "CREATE TABLE " + name + " (\n";
		ListIterator<Column> iterator = columns.listIterator();
		int count = 0;
		while(iterator.hasNext()) {
			Column column = iterator.next();
			tableStr += "   " + column;
			if(count < columns.size() - 1)
				tableStr += ",";
			tableStr += "\n";
			count++;
		}
		tableStr += ");\n";
		return tableStr;
	}
	
	public String toSql() {
		String tableStr = "CREATE TABLE " + name + " (\n";
		ListIterator<Column> iterator = columns.listIterator();
		int count = 0;
		while(iterator.hasNext()) {
			Column column = iterator.next();
			tableStr += "   " + column.toSql();
			if(count < columns.size() - 1)
				tableStr += ",";
			tableStr += "\n";
			count++;
		}
		tableStr += ");\n";
		return tableStr;
	}
}
