import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

/** * @author Wael Abouelsaadat */ 

public class SQLTerm {

	public String _strTableName,_strColumnName, _strOperator;
	public Object _objValue;

	// public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
	// 	this._strTableName = _strTableName;
	// 	this._strColumnName = _strColumnName;
	// 	this._strOperator = _strOperator;
	// 	this._objValue = _objValue;
	// }
	public static Iterator<Row> searchIterator(Vector<Vector<Row>> res, String op, Hashtable<String, Object> myKeys,String tableName) throws DBAppException {
		Vector<Row> fin = new Vector<>();
		switch (op.toUpperCase()) {
			case "AND":
				for (Vector<Row> rows : res) {
					for (Row row : rows) {
						boolean flag = true;
						for (String columnName : myKeys.keySet()) {
							Object value = myKeys.get(columnName);
							if (!row.getValue(columnName).equals(value)) {
								flag = false;
								break;
							}
						}
						if (flag) {
							fin.add(row);
						}
					}
				}
				break;
			case "OR":
				for (Vector<Row> rows : res) {
					for (Row row : rows) {
						boolean flag = false;
						for (String columnName : myKeys.keySet()) {
							Object value = myKeys.get(columnName);
							if (row.getValue(columnName).equals(value)) {
								flag = true;
								break;
							}
						}
						if (flag) {
							fin.add(row);
						}
					}
				}
				break;
			case "XOR":
				for (Vector<Row> rows : res) {
					for (Row row : rows) {
						int count = 0;
						for (String columnName : myKeys.keySet()) {
							Object value = myKeys.get(columnName);
							if (row.getValue(columnName).equals(value)) {
								count++;
							}
						}
						if (count == 1) {
							fin.add(row);
						}
					}
				}
				break;
			default:
				throw new DBAppException("Invalid Operation");
			
		}
		String pk = DBApp.getClusterKey(tableName);

			
			
		
	
	
		return removeDup(fin,pk);
	}
		public static Iterator<Row> removeDup(Vector<Row> fin,String pk) throws DBAppException{
			Vector<Row> fin2 = new Vector<>();
			for (Row row : fin) {
				if (fin2.isEmpty()) {
					fin2.add(row);
				} else {
					Iterator<Row> iterator = fin2.iterator();
					boolean duplicate = false;
					while (iterator.hasNext()) {
						Row k = iterator.next();
						int com = Table.comparePKs(k.getValue(pk.trim()).toString(), row.getValue(pk.trim()));
						if (com == 0) {
							duplicate = true;
							break;
						}
					}
					if (!duplicate) {
						fin2.add(row);
					}
				}
			}
			return fin2.iterator();
		
		}
	}

