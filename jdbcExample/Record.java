package cs5200.edu.neu.cs5200.jga.jdbc;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;

public class Record {
	private Hashtable<String, Field> fields;
	private String tableName;
	public Record(String tableName) {
		this.tableName = tableName;
		this.fields = new Hashtable<String, Field>();
	}
	public Record(String tableName, Hashtable<String, Field> fields) {
		this(tableName);
		this.fields = fields;
	}
	public Record(String tableName, List<Field> fields) {
		this(tableName);
		setFields(fields);
	}
	public Record(String tableName, Field[] fields) {
		this(tableName);
		setFields(fields);
	}
	public List<Field> getFields() {
		return new ArrayList<Field>(fields.values());
	}
	public void setFields(List<Field> fields) {
		ListIterator<Field> iter = fields.listIterator();
		while(iter.hasNext()) {
			Field field = iter.next();
			this.fields.put(field.getName(), field);
		}
	}
	public void setFields(Field[] fields) {
		for(Field field : fields) {
			this.fields.put(field.getName(), field);
		}
	}
	public Field getField(String name) {
		return fields.get(name);
	}
	public void setField(String name, Field value) {
		this.fields.put(name, value);
	}
	public void setFieldValue(String fieldName, Object value) {
		this.fields.get(fieldName).setValue(value);
	}
	public Object getFieldValue(String fieldName) {
		return this.fields.get(fieldName).getValue();
	}
	public String toString() {
		String cols = "(";
		String vals = "(";
		String insertSql = "INSERT INTO " + this.tableName + "\n";
		insertSql += cols + "\n";
		insertSql += "VALUES\n";
		insertSql += vals;
		return insertSql;
	}
}
