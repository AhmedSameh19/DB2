
/** * @author Wael Abouelsaadat */ 

import java.io.*;
import java.util.*;


public class DBApp {
	HashSet <String> tableNames=new HashSet<>();


	public DBApp( ) throws DBAppException{
		this.init();
	}
 
	public void init() throws DBAppException{
		Properties props = new Properties();
        try {
            InputStream input = new FileInputStream("starter-code\\resources\\DBApp.config");
            props.load(input);
            Table.PAGE_SIZE = Integer.parseInt(props.getProperty("MaximumRowsCountinPage"));
        } catch (Exception e) {
            throw new DBAppException(e);
        }
        File file = new File("metadata.csv");
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new DBAppException(e);
            }
        }
        File tablesFolder = new File("Tables");
        if (!tablesFolder.isDirectory())
            tablesFolder.mkdir();
        File pagesFolder = new File("Pages");
        if (!pagesFolder.isDirectory())
            pagesFolder.mkdir();
		
	}
public void writeNewColumn( ArrayList<String[]> metadata) throws DBAppException {
        FileWriter fw = null;
        BufferedWriter bw = null;
        PrintWriter pw = null;
        String filename = "metadata.csv";
        try {
            fw = new FileWriter(filename, true);
            bw = new BufferedWriter(fw);
            pw = new PrintWriter(bw);
       for (String[] row : metadata) {
                pw.println(String.join(", ", row));
            }
            pw.flush();


        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        } finally {
            try {
                pw.close();
                bw.close();
                fw.close();
            } catch (Exception e) {

                throw new DBAppException();
            }
        }
    }
	public void readTableNames() throws DBAppException {
        BufferedReader br = null;
        String itemsPath = "metadata.csv";
        try {
            br = new BufferedReader(new FileReader(itemsPath));
            String line = br.readLine();
            while (line != null) {
                String[] data = line.split(",");
                String name = data[0];
                tableNames.add(name);
                line = br.readLine();
            }
        } catch (Exception e) {
            throw new DBAppException(e);
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                throw new DBAppException(e);
            }
        }
    
    }
	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException{
								if (tableNames.size() == 0)
									readTableNames();
								if(tableNames.contains(strTableName)){
									throw new DBAppException("Already exists in table!!!");

								}else{
									for (String key : htblColNameType.keySet()) {
										String value = htblColNameType.get(key);
										if(!(value.equals("java.lang.double")||value.equals("java.lang.String")||value.equals("java.lang.Integer"))){
											throw new DBAppException("Data type mismatch!");
	
										}
									}
										Table t = new Table(strTableName);
	
										try{
											ArrayList<String[]> metaData=new ArrayList<>();
											for (String myKey : htblColNameType.keySet()) {
												String myValue = htblColNameType.get(myKey);
												Boolean clusterKey=myKey.equals(strClusteringKeyColumn)?true:false;
												metaData.add(new String[]{strTableName, myKey, myValue,String.valueOf(clusterKey) ,"null", "null"});
												
												
										}
										FileOutputStream fileOut = new FileOutputStream("Tables/" + strTableName + ".class");
										   ObjectOutputStream out = new ObjectOutputStream(fileOut);
										   out.writeObject(t);
										   out.close();
										   fileOut.close();
										writeNewColumn(metaData);
		
	
									}
									catch (Exception e) {
										throw new DBAppException(e.getMessage());
									}					
									tableNames.add(strTableName);
									}
								}
								

	


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		throw new DBAppException("not implemented yet");
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException{
		try{
			
			String pk = getClusterKey(strTableName);
			ArrayList<Hashtable<String, String>> dataOfTable = readCsv(strTableName); //type max min pk
			Hashtable<String, String> htblColNameType = dataOfTable.get(0);
			if (!(htblColNameValue.keySet().contains(pk.trim()))) {
				throw new DBAppException("CANT INSERT WITHOUT PK");
			}
			for (String str : htblColNameType.keySet()) {
				if (!htblColNameValue.keySet().contains(str)) {
					htblColNameValue.put(str, values.NULL);
				}
			}
			verifyRow(strTableName, htblColNameValue);
			Table t1 = getTable(strTableName);
			Row t = new Row(htblColNameValue, strTableName);
			t1.insertIntoTable(t);
			saveTable(t1);
		}
		catch(DBAppException e){
			throw new DBAppException("Error inserting record: " + e.getMessage());

		}
									
	}


	public Table getTable(String strTableName) throws DBAppException {
		try {
            Table t = null;
            FileInputStream fileIn = new FileInputStream("Tables/" + strTableName + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            t = (Table) in.readObject();
            in.close();
            fileIn.close();
            return t;
        } catch (java.io.FileNotFoundException e) {
            throw new DBAppException("File for table '" + strTableName + "' not found.");
        } catch (java.io.IOException e) {
            throw new DBAppException("Error reading file for table '" + strTableName + "'.");
        } catch (ClassNotFoundException e) {
            throw new DBAppException("Class definition for Table not found during deserialization.");
        }
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
								try {
									verifyPK(strTableName,strClusteringKeyValue);
									verifyRow(strTableName, htblColNameValue);
									Table t = getTable(strTableName);
									Row tuple = new Row(htblColNameValue, strTableName);
									t.updateTable(tuple, strClusteringKeyValue);
									saveTable(t);
								} catch (Exception e) {
									throw new DBAppException("Error updating record: " + e.getMessage());
								}
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
									try {
										verifyRow(strTableName, htblColNameValue);
										Table t = getTable(strTableName);
										t.deleteRecord(htblColNameValue);
										saveTable(t);
									} catch (DBAppException e) {
										throw new DBAppException("Error deleting record: " + e.getMessage());
									}	
								}


	public Iterator<Row> selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException, IOException{
										try {
											String _strTableName,_strColumnName, _strOperator;
		Object _objValue;
		Vector<Vector<Row>> res=new Vector<>();
		String op=strarrOperators[0];
		Hashtable <String,Object> myKeys=new Hashtable<>();
		String name="";
		for(int i =0;i<arrSQLTerms.length;i++){
			_strTableName=arrSQLTerms[i]._strTableName.trim();
			_strOperator=arrSQLTerms[i]._strOperator.trim();
			_strColumnName=arrSQLTerms[i]._strColumnName.trim();
			_objValue=arrSQLTerms[i]._objValue;
			if(!tableNames.contains(_strTableName.trim())){
				throw new DBAppException("Table not avaliable");
			}
			myKeys.put(_strColumnName, _objValue);
			Table t=getTable(_strTableName);
			
			res.add(t.searchTable( _strColumnName, _strOperator, _objValue));
			name=_strTableName;
		}	

		return SQLTerm.searchIterator(res,op,myKeys,name);
										} catch (DBAppException e) {
											throw new DBAppException("Error in selecting this SQL query: " + e.getMessage());

										}
		
						
									
	}
	
    public void saveTable(Table t) throws DBAppException {
        try {
            // Serialize the object to a file
            FileOutputStream fileOut = new FileOutputStream("Tables/" + t.name + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(t);
            out.close();
            fileOut.close();
            t = null;
            System.gc();
        } catch (IOException e) {
            throw new DBAppException("Error serializing table '" + t.name + "'.");
        }
    }
    public static  String getClusterKey(String strTableName) throws DBAppException {
        BufferedReader br = null;
        String itemsPath = "metadata.csv";
        String ck = "";
        try {
            br = new BufferedReader(new FileReader(itemsPath));
            String line = br.readLine();
            while (line != null) {
                String[] data = line.split(",");
                String name = data[0];
                if (name.equals(strTableName)) {
                    if ((data[3].trim()).equalsIgnoreCase("true")){

					

                        ck=data[1];
					}
                }
                line = br.readLine();
            }
			br.close();
            return ck;
			
        } 
		catch (Exception e) {
            throw new DBAppException("Error while getting the cluster key: "+e.getMessage());
        }
		finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException e) {
				throw new DBAppException("Error closing BufferedReader: " + e.getMessage());
			}
		}
		

    }

	public static void verifyRow(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		ArrayList<Hashtable<String, String>> dataOfTable = readCsv(strTableName); 
        Hashtable<String, String> htblColNameType = dataOfTable.get(0);
        String pk = dataOfTable.get(1).get("pk").trim();
		

        for (String str : htblColNameValue.keySet()) {
            if (!htblColNameType.keySet().contains(str))
                throw new DBAppException("THIS COLUMN DOESNT EXIST");
        }
        for (String str : htblColNameType.keySet()) {
            if (htblColNameValue.get(str) == values.NULL && !str.equals(pk))
                continue;
            else if (htblColNameValue.get(str)==values.NULL && str.equals(pk))
                throw new DBAppException("PK CAN'T BE NULL");
            if (htblColNameType.get(str).toLowerCase().equals("java.lang.string")) {
                if (!(htblColNameValue.get(str) instanceof String))
                    throw new DBAppException("Invalid DataTypes");
               
            } else if (htblColNameType.get(str).toLowerCase().equals("java.lang.integer")) {
                if (!(htblColNameValue.get(str) instanceof Integer))
                    throw new DBAppException("Invalid DataTypes");
                
            } else if (htblColNameType.get(str).toLowerCase().equals("java.lang.double")) {
                if (!(htblColNameValue.get(str) instanceof Double || htblColNameValue.get(str) instanceof Integer))
                    throw new DBAppException("Invalid DataTypes");
               
            }
            

        }
    }

	public void verifyPK(String strTableName, String strClusteringKeyValue) throws DBAppException {
        try {

		ArrayList<Hashtable<String, String>> data = readCsv(strTableName);
		Hashtable<String, String>  pkdata = data.get(1);
        String pkType = pkdata.get("type");
        if (pkType.toLowerCase().equals("java.lang.string")) {
            return;
        } else if (pkType.toLowerCase().equals("java.lang.integer")) {
            return;
        } else if (pkType.toLowerCase().equals("java.lang.double")) {
           return;
        } 
	}
		catch (Exception e) {
			throw new DBAppException("Wrong PK: " + e.getMessage());
				}
    }
	public static ArrayList<Hashtable<String, String>> readCsv(String tableName) throws DBAppException {
		Hashtable<String, String> htblColNameType = new Hashtable<>();
        Hashtable<String, String> pk = new Hashtable<>();
		String csvFile="metadata.csv";
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            String cvsSplitBy = ",";
            while ((line = br.readLine()) != null) {
                String[] data = line.split(cvsSplitBy);
				htblColNameType.put(data[1].trim(),data[2]);
                if (data[3].trim().equalsIgnoreCase("true") && data[0].trim().compareTo(tableName)==0  ) {
                    pk.put("pk",data[1]);  
					pk.put("type",data[2]);

                }
            }
			ArrayList<Hashtable<String, String>> res = new ArrayList<>();
			res.add(htblColNameType);
			res.add(pk);
			return res;
        } catch (IOException e) {
			throw new DBAppException("Error reading the CSV file: "+e.getMessage());
		        }

    }
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (String tableName : tableNames) {
			try {
				sb.append(getTable(tableName)).append("\n");
			} catch (DBAppException e) {
				try {
					throw new DBAppException("Error getting table name: " + e.getMessage());
				} catch (DBAppException e1) {
					e1.printStackTrace();
				}
			}
		}
		return sb.toString();
	}
	


	public static void main( String[] args ) throws DBAppException, IOException{


		DBApp	dbApp = new DBApp( );
			String strTableName = "Student";
			try{
				Hashtable <String,String> htblColNameType = new Hashtable<>( );
				htblColNameType.put("id", "java.lang.Integer");
				htblColNameType.put("name", "java.lang.String");
				htblColNameType.put("gpa", "java.lang.double");
				dbApp.createTable( strTableName, "id", htblColNameType );
			}
			catch( Exception ex ){
				System.out.println(ex.toString());
			}
		// 	//dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
			try{
			  Hashtable<String, Object> htblColNameValue = new Hashtable<>();
			 htblColNameValue.put("id", Integer.valueOf(2343432));
			 htblColNameValue.put("name", "Ahmed Noor");
			 htblColNameValue.put("gpa", Double.valueOf(0.95));
			 dbApp.insertIntoTable(strTableName, htblColNameValue);

			 htblColNameValue.clear( );
			 htblColNameValue.put("id", Integer.valueOf( 453455 ));
			 htblColNameValue.put("name", new String("Ahmed tarek" ) );
			 htblColNameValue.put("gpa", Double.valueOf( 0.95 ) );
			 dbApp.insertIntoTable( strTableName , htblColNameValue );

			 htblColNameValue.clear( );
			 htblColNameValue.put("id", Integer.valueOf( 453452325 ));
			 htblColNameValue.put("name", new String("Ahmed khaled" ) );
			 htblColNameValue.put("gpa", Double.valueOf( 1.9 ) );
			 dbApp.insertIntoTable( strTableName , htblColNameValue );

			 htblColNameValue.clear( );
			 htblColNameValue.put("id", Integer.valueOf( 23498 ));
			 htblColNameValue.put("name", new String("John Noor" ) );
			 htblColNameValue.put("gpa", Double.valueOf( 1.0 ) );
			 dbApp.insertIntoTable( strTableName , htblColNameValue );

			 htblColNameValue.clear( );
			 htblColNameValue.put("id", Integer.valueOf( 23499 ));
			 htblColNameValue.put("name", new String("John Noor" ) );
			 htblColNameValue.put("gpa", Double.valueOf( 0.88 ) );
			 dbApp.insertIntoTable( strTableName , htblColNameValue );
			 System.out.println(dbApp);

			 htblColNameValue.clear( );
			 htblColNameValue.put("name", new String("John Noor" ) );
			 htblColNameValue.put("gpa", Double.valueOf( 1.9 ) );

			 dbApp.updateTable(strTableName,"23498", htblColNameValue );
			 System.out.println(dbApp);

			 htblColNameValue.clear( );
			 htblColNameValue.put("id", Integer.valueOf( 23499 ));
			 htblColNameValue.put("name", new String("John Noor" ) );
			 htblColNameValue.put("gpa", Double.valueOf( 0.88 ) );
			 
			 
			 dbApp.deleteFromTable("Student",htblColNameValue);
			//  System.out.println(dbApp);
			 htblColNameValue.clear( );
			 htblColNameValue.put("id", Integer.valueOf( 23498 ));
			 htblColNameValue.put("name", new String("John Noor" ) );
			 htblColNameValue.put("gpa", Double.valueOf( 1.0 ) );

			dbApp.deleteFromTable("Student",htblColNameValue);
			System.out.println(dbApp);

			 SQLTerm[] arrSQLTerms;
			 arrSQLTerms = new SQLTerm[2];
			 
			 // Initialize elements before accessing fields
			 arrSQLTerms[0] = new SQLTerm();
			 arrSQLTerms[0]._strTableName = "Student";
			 arrSQLTerms[0]._strColumnName = "name";
			 arrSQLTerms[0]._strOperator = "=";
			 arrSQLTerms[0]._objValue = "Ahmed Noor";
			 
			 arrSQLTerms[1] = new SQLTerm();
			 arrSQLTerms[1]._strTableName = "Student";
			 arrSQLTerms[1]._strColumnName = "gpa";
			 arrSQLTerms[1]._strOperator = "=";
			 arrSQLTerms[1]._objValue = new Double(0.95);
			 
			 String[] strarrOperators = new String[1];
			 
			 strarrOperators[0] = "AND"; 
			 // select * from Student where name = “John Noor” or gpa = 1.5; 
			 Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators); 
			 while (resultSet.hasNext()) {
				System.out.println(resultSet.next());
			}
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}
}
	

	