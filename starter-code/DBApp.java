
/** * @author Wael Abouelsaadat */ 

import java.io.*;
import java.util.*;

public class DBApp {
	static HashSet <String> tableNames=new HashSet<>();


	public DBApp( ) throws DBAppException{
		this.init();
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init() throws DBAppException{
		Properties props = new Properties();
        try {
            InputStream input = new FileInputStream("starter-code\\resources\\DBApp.config");
            props.load(input);
            Table.PAGE_SIZE = Integer.parseInt(props.getProperty("MaximumRowsCountinPage"));
            System.out.println(Table.PAGE_SIZE);
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
								System.out.println(tableNames);
								if (tableNames.size() == 0)
									readTableNames();
								if(tableNames.contains(strTableName)){
									throw new DBAppException("Already exists in table!!!");

								}else{
									for (String key : htblColNameType.keySet()) {
										String value = htblColNameType.get(key);
										if(!(value.equals("java.lang.double")||value.equals("java.lang.String")||value.equals("java.lang.Integer"))){
											throw new DBAppException("Data type mismatch!");
	
										}}
										Table t = new Table(strTableName);
	
										try{
											ArrayList<String[]> metaData=new ArrayList<>();
											for (String myKey : htblColNameType.keySet()) {
												String myValue = htblColNameType.get(myKey);
												Boolean clusterKey=myKey.equals(strClusteringKeyColumn)?true:false;
												metaData.add(new String[]{strTableName, myKey, myValue,String.valueOf(clusterKey) , null,null});
												
												
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
									System.out.println(tableNames);
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
		String pk = getClusterKey(strTableName);
        if (!htblColNameValue.keySet().contains(pk)) {
            throw new DBAppException("CANT INSERT WITHOUT PK");
        }
		
									
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException{
										
		return null;
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
            throw new DBAppException(e);
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
            throw new DBAppException(e);
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


	public static void main( String[] args ) throws DBAppException{

	try{
		DBApp	dbApp = new DBApp( );
			String strTableName = "Student";
			Hashtable <String,String> htblColNameType = new Hashtable<>( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			dbApp.createTable( strTableName, "id", htblColNameType );
			// dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

			Hashtable<String, Object> htblColNameValue = new Hashtable<>();
			htblColNameValue.put("id", Integer.valueOf(2343432));
			htblColNameValue.put("name", "Ahmed Noor");
			htblColNameValue.put("gpa", Double.valueOf(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", Integer.valueOf( 453455 ));
			// htblColNameValue.put("name", new String("Ahmed Noor" ) );
			// htblColNameValue.put("gpa", Double.valueOf( 0.95 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", Integer.valueOf( 5674567 ));
			// htblColNameValue.put("name", new String("Dalia Noor" ) );
			// htblColNameValue.put("gpa", Double.valueOf( 1.25 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", Integer.valueOf( 23498 ));
			// htblColNameValue.put("name", new String("John Noor" ) );
			// htblColNameValue.put("gpa", Double.valueOf( 1.5 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", Integer.valueOf( 78452 ));
			// htblColNameValue.put("name", new String("Zaky Noor" ) );
			// htblColNameValue.put("gpa", Double.valueOf( 0.88 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );


			// SQLTerm[] arrSQLTerms;
			// arrSQLTerms = new SQLTerm[2];
			// arrSQLTerms[0]._strTableName =  "Student";
			// arrSQLTerms[0]._strColumnName=  "name";
			// arrSQLTerms[0]._strOperator  =  "=";
			// arrSQLTerms[0]._objValue     =  "John Noor";

			// arrSQLTerms[1]._strTableName =  "Student";
			// arrSQLTerms[1]._strColumnName=  "gpa";
			// arrSQLTerms[1]._strOperator  =  "=";
			// arrSQLTerms[1]._objValue     =  Double.valueOf( 1.5 );

			// String[]strarrOperators = new String[1];
			// strarrOperators[0] = "OR";
			// // select * from Student where name = "John Noor" or gpa = 1.5;
			// Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

}