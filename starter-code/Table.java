import java.io.*;
import java.util.*;

public class Table implements Serializable {
    static int PAGE_SIZE;
    String name;
    Vector<Integer> pageNums;
    Hashtable<String, String> indexNames;

    public Table(String name) {
        this.name = name;
        this.pageNums = new Vector<>();
        this.indexNames = new Hashtable<>();
    }

    public boolean inTable(Row row) throws DBAppException {
        boolean flag = false;
        for (int i = 0; i < pageNums.size(); i++) {
            Page p = getPageByNumber(pageNums.get(i));
    
            int low = 0;
            int high = p.getTuples().size() - 1;
            int mid;
    
            while (low <= high) {
                mid = low + (high - low) / 2;
                Row tempTup = p.getTuples().get(mid);
                int com = row.compareTo(tempTup);
                if (com == 0) {
                    flag = true;
                    return true; 
                } else if (com < 0) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
            savePage(p);
        }
        return flag;
    }
    

    public void insertIntoTable(Row record) throws DBAppException {
        if (this.pageNums.size() == 0) {
            createPage();
            Page p = getPageByNumber(this.pageNums.get(0));
            p.getTuples().add(record);
            p.setName("Pages/" + this.name + "" + p.getPageNumber() + ".class");
            savePage(p);
            return;
        }
        Page p = getPageByTuple(record);

        if (p == null) {
            Page p3 = getPageByNumber(this.pageNums.get(this.pageNums.size() - 1));
            if (p3.size() < Table.PAGE_SIZE) {

                p3.getTuples().add(record);
                p3.setName("Pages/" + this.name + "" + p3.getPageNumber() + ".class");

                savePage(p3);
                return;
            } else {

                createPage();

                Page p2 = getPageByNumber(this.pageNums.get(this.pageNums.size() - 1));

                p2.getTuples().add(record);
                p2.setName("Pages/" + this.name + "" + p2.getPageNumber() + ".class");

                savePage(p2);
                return;
            }

        } else {

            Row first = p.getTuples().get(0);
            if (record.compareTo(first) < 0) {
                for (int i = 0; i < pageNums.size(); i++) {
                    int pgNum = pageNums.get(i);
                    if (pgNum == p.getPageNumber() && i != 0) {
                        Page prevPage = getPageByNumber(pgNum - 1);

                        if (prevPage.getTuples().size() < Table.PAGE_SIZE) {

                            prevPage.getTuples().add(record);
                            prevPage.setName("Pages/" + this.name + "" + prevPage.getPageNumber() + ".class");

                            savePage(prevPage);
                            return;
                        }
                    }
                }
            }
        }

        this.insertRec(record, p);
        if (this.indexNames.size() > 0)
            this.insertBTree(record);

    }

    public void insertBTree(Row record) throws DBAppException {

        for (Object key : indexNames.keySet()) {
            String indexTableName = indexNames.get(key);
            BPlusTree t = getTree(indexTableName);
            Vector<Row> returned = new Vector<>();

            Object k = record.getValue(key);
            if (k instanceof String) {
                String k0 = (String) k.toString();

                returned = (Vector<Row>) t.search(k0);
            } else if (k instanceof Double) {
                Double k1 = (Double) k;

                returned = (Vector<Row>) t.search(k1);
            } else if (k instanceof Integer) {
                Integer k2 = (Integer) k;
                returned = (Vector<Row>) t.search(k2);
            } else {
                throw new DBAppException("Unsupported key type");
            }
            if (returned == null) {
                Vector<Row> x = new Vector<>();
                x.add(record);
                Object z = record.getValue(key);
                if (z instanceof String) {
                    String k3 = (String) z.toString();
                    t.insert(k3, x);
                } else if (z instanceof Double) {
                    Double k4 = (Double) z;
                    t.insert(k4, x);
                } else if (z instanceof Integer) {
                    Integer k5 = (Integer) z;
                    t.insert(k5, x);
                } else {
                    throw new DBAppException("Unsupported key type");
                }
            } else {
                returned.add(record);
                Object z = record.getValue(key);
                if (z instanceof String) {
                    String k8 = (String) z.toString();
                    t.insert(k8, returned);
                } else if (z instanceof Double) {
                    Double k6 = (Double) z;
                    t.insert(k6, returned);
                } else if (z instanceof Integer) {
                    Integer k7 = (Integer) z;
                    t.insert(k7, returned);
                } else {
                    throw new DBAppException("Unsupported key type");
                }
            }
            saveTree(t, indexTableName);
        }

    }

    public static BPlusTree getTree(String treeName) throws DBAppException {
        try {
            BPlusTree p = null;
            FileInputStream fileIn = new FileInputStream("Index/" + treeName + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (BPlusTree) in.readObject();
            in.close();
            fileIn.close();
            return p;
        } catch (Exception e) {
            throw new DBAppException("This Tree not avaliable: " + e.getMessage());
        }

    }

    private void insertRec(Row record, Page p) throws DBAppException {
        Vector<Row> tuples = p.getTuples();
        int low = 0, high = tuples.size() - 1, mid = (low + high) / 2;
        while (low <= high) {
            mid = (low + high) / 2;
            Row tempTup = tuples.get(mid);
            int com = record.compareTo(tempTup);
            if (com == 0) {
                throw new DBAppException("Existing PK");
            }
            if (com < 0)
                high = mid - 1;
            else
                low = mid + 1;
        }
        if (tuples.isEmpty()) {

            tuples.add(record);
            p.setRow(tuples);
            p.setName("Pages/" + this.name + "" + p.getPageNumber() + ".class");

            savePage(p);
            return;
        }

        // now we insert it at mid value and shift all other record on cell down
        Row tempTup = tuples.get(mid);
        if (record.compareTo(tempTup) > 0)
            mid++;
        // if the page was full before inserting
        if (tuples.size() == Table.PAGE_SIZE) {
            // insert the last record to the next page
            if (p.getPageNumber() == this.pageNums.get(this.pageNums.size() - 1))
                createPage();
            int pageNum = 0;
            for (int i = 0; i < pageNums.size(); i++) {
                int pgNum = pageNums.get(i);
                if (p.getPageNumber() == pgNum) {
                    pageNum = pageNums.get(i + 1);
                    break;
                }
            }
            Page nextPage = getPageByNumber(pageNum); // edit
            this.insertRec(tuples.get(tuples.size() - 1), nextPage);
            for (int i = tuples.size() - 1; i > mid; i--) {
                tuples.set(i, tuples.get(i - 1));
            }

            tuples.set(mid, record);
            p.setRow(tuples);
            p.setName("Pages/" + this.name + "" + p.getPageNumber() + ".class");

            savePage(p);
            return;
        }
        Row last = tuples.get(tuples.size() - 1);
        for (int i = tuples.size() - 1; i > mid; i--) {
            tuples.set(i, tuples.get(i - 1));
        }
        tuples.set(mid, record);
        tuples.add(last);
        p.setRow(tuples);
        p.setName("Pages/" + this.name + "" + p.getPageNumber() + ".class");

        savePage(p);
    }

    public Page getPageByTuple(Row t) throws DBAppException {
        for (int i = 0; i < this.pageNums.size(); i++) {
            int pgNum = pageNums.get(i);
            Page p = getPageByNumber(pgNum);
            if (p.getTuples().get(p.getTuples().size() - 1).compareTo(t) > 0) {
                return p;
            }
        }
        return null;
    }

    public void createPage() throws DBAppException {
        if (pageNums.size() == 0) {
            Page p = new Page(1);
            this.pageNums.add(1);
            p.setName("Pages/" + this.name + "" + p.getPageNumber() + ".class");

            savePage(p);
            return;
        }
        Page p = new Page(pageNums.get(pageNums.size() - 1) + 1);
        this.pageNums.add(p.getPageNumber());
        p.setName("Pages/" + this.name + "" + p.getPageNumber() + ".class");

        savePage(p);
    }

    public void savePage(Page p) throws DBAppException {
        String filePath = p.getName();
        try (FileOutputStream fileOut = new FileOutputStream(filePath);
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(p);
            out.flush();
            out.close();

        } catch (IOException e) {
            throw new DBAppException(e);
        }
    }

    public Page getPageByNumber(int pageNumber) throws DBAppException {
        try {
            Page p = null;
            FileInputStream fileIn = new FileInputStream("Pages/" + this.name + "" + pageNumber + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Page) in.readObject();
            in.close();
            fileIn.close();
            return p;
        } catch (Exception e) {
            throw new DBAppException(e);
        }
    }

    public Page updateme(String strClusteringKeyValue) throws DBAppException {
        String pk = DBApp.getClusterKey(this.name).trim();
        for (int i = 0; i < pageNums.size(); i++) {
            Page pg = getPageByNumber(pageNums.get(i));
            System.out.println(pg);
            Object val0 = pg.getTuples().get(0).getData().get(pk);
            Object val1 = pg.getTuples().get(pg.getTuples().size() - 1).getData().get(pk);

            if (comparePKs(strClusteringKeyValue.trim(), val0) >= 0 && comparePKs(strClusteringKeyValue.trim(), val1) <= 0) {
                return pg;
            }
        }
        return null;
    }

    public void updateTable(Row record, String strClusteringKeyValue) throws DBAppException {
        String pk = DBApp.getClusterKey(this.name).trim();
        Page pg = updateme(strClusteringKeyValue);
        if (pg == null || pk == null) {
            throw new DBAppException("DOESNT EXIST");
        }
        int low = 0, high = pg.getTuples().size() - 1, mid = (low + high) / 2;
        while (low <= high) {
            mid = (low + high) / 2;
            Row tempTup = pg.getTuples().get(mid);
            Object midVal = tempTup.getValue(pk);
            int com = comparePKs(strClusteringKeyValue, midVal);
            if (com == 0) {
                for (String str : record.getData().keySet()) {
                    tempTup.getData().replace(str, record.getData().get(str));
                }
                pg.setName("Pages/" + this.name + "" + pg.getPageNumber() + ".class");

                savePage(pg);
                updateBTree(record, strClusteringKeyValue);
                return;
            }
            if (com < 0)
                high = mid - 1;
            else
                low = mid + 1;
        }
        throw new DBAppException("PK DOESNT EXIST");

    }

    private void updateBTree(Row record, String strClusteringKeyValue) throws DBAppException {
        for (Object key : indexNames.keySet()) {
            String indexTableName = indexNames.get(key);
            BPlusTree t = getTree(indexTableName);
            Vector<Row> returned = new Vector<>();
            Object k = record.getData().get(key);
            if (k instanceof String) {
                String k0 = (String) k.toString();
                returned = (Vector<Row>) t.search(k0);
            } else if (k instanceof Double) {
                Double k1 = (Double) k;
                returned = (Vector<Row>) t.search(k1);
            } else if (k instanceof Integer) {
                Integer k2 = (Integer) k;
                returned = (Vector<Row>) t.search(k2);
            } else {
                throw new DBAppException("Unsupported key type");
            }
            String pk = DBApp.getClusterKey(this.name);
            for (Row r : returned) {
                if (comparePKs(strClusteringKeyValue, (r.getData().get(pk.trim()))) == 0) {
                    returned.remove(r);
                    returned.add(record);
                    Object z = k;
                    if (z instanceof String) {
                        String k8 = (String) z.toString();
                        t.insert(k8, returned);
                    } else if (z instanceof Double) {
                        Double k6 = (Double) z;
                        t.insert(k6, returned);
                    } else if (z instanceof Integer) {
                        Integer k7 = (Integer) z;
                        t.insert(k7, returned);
                    } else {
                        throw new DBAppException("Unsupported key type");
                    }
                }
            }
            saveTree(t, indexTableName);
        }
    }

    public static int comparePKs(String stringPK, Object realPK) throws DBAppException {
        
        if (realPK instanceof String) {
            return stringPK.compareTo((String) realPK);
        } else if (realPK instanceof Integer) {
            Integer sPK = Integer.parseInt(stringPK);
            Integer rPK = (Integer) realPK;
            return sPK.compareTo(rPK);
        } else if (realPK instanceof Double) {
            Double sPK = Double.parseDouble(stringPK);
            Double rPK = (Double) realPK;
            return sPK.compareTo(rPK);
        }

        throw new DBAppException("WRONG TYPE FOR PRIMARY KEY!");
    }

    public Vector<Row> searchTable(String _strColumnName, String _strOperator, Object _objValue) throws DBAppException {
        Vector<Row> res = new Vector<>();
        String op = identifyOperator(_strOperator);
        Object val;
        if (op == null)
            throw new DBAppException("Wrong operator");

        if (_objValue instanceof String) {
            val = (String) _objValue;
        } else if (_objValue instanceof Double) {
            val = (Double) _objValue;
        } else if (_objValue instanceof Integer) {
            val = (Integer) _objValue;
        } else {
            throw new DBAppException("Unsupported data type");
        }
        for (int i = 0; i < pageNums.size(); i++) {
            Page p = getPageByNumber(i + 1);
            for (int j = 0; j < p.size(); j++) {
                Row r = p.getTuples().get(j);
                switch (_strOperator) {
                    case "=":
                        if (r.getValue(_strColumnName).equals(val)) {
                            res.add(r);
                        }
                        break;
                    case ">=":
                        if ((Double) r.getValue(_strColumnName) >= (Double) val) {
                            res.add(r);
                        }
                        break;
                    case ">":
                        if ((Double) r.getValue(_strColumnName) > (Double) val) {
                            res.add(r);
                        }
                        break;
                    case "<=":
                        if ((Double) r.getValue(_strColumnName) <= (Double) val) {
                            res.add(r);
                        }
                        break;
                    case "<":
                        if ((Double) r.getValue(_strColumnName) < (Double) val) {
                            res.add(r);
                        }
                        break;
                    case "!=":
                        if (!r.getValue(_strColumnName).equals(val)) {
                            res.add(r);
                        }
                        break;
                    default:
                        throw new DBAppException("Unsupported operator");
                }
            }
        }
        return res;
    }

    public String identifyOperator(String expression) {
        String[] operators = { ">", ">=", "<", "<=", "!=", "=" };
        for (String op : operators) {
            if (expression.contains(op)) {
                return op;
            }
        }
        return null;
    }

    public void updatePagesNumbers() throws DBAppException {
        for (int i = 0; i < pageNums.size(); i++) {
            int pageNumber = pageNums.get(i);
            Page page = getPageByNumber(pageNumber);
            if (page == null) {
                throw new DBAppException("Page with number " + pageNumber + " not found.");
            }
            String newPath = "Pages/" + this.name + "" + (pageNumber - 1) + ".class";
            page.setName(newPath);
            page.setPageNumber(pageNumber - 1);
            pageNums.set(i, pageNumber - 1);
            savePage(page);
            String path = "Pages/" + this.name + "" + pageNumber + ".class";
            File file = new File(path);
            if (!file.delete()) {
                throw new DBAppException("Failed to delete file: " + path);
            }
        }
    }

    public void deleteRow(int index, Page p) throws DBAppException {
        p.getTuples().remove(index);
        if (p.getTuples().isEmpty()) {
            String path = "Pages/" + this.name + "" + p.getPageNumber() + ".class";
            File file = new File(path);
            if (!file.delete()) {
                throw new DBAppException("Failed to delete file: " + path);
            }
            pageNums.remove(Integer.valueOf(p.getPageNumber()));
            updatePagesNumbers();
        } else {
            p.setName("Pages/" + this.name + "" + p.getPageNumber() + ".class");
            savePage(p);
        }
    }

    public void deleteRecord(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        String pk = DBApp.getClusterKey(this.name).trim();
        String strClusteringKeyValue = htblColNameValue.get(pk.trim()).toString();
        Page pg = findPage(strClusteringKeyValue.trim());
        if (pk == null || pg == null) {
            throw new DBAppException("Check your inputs!!!");
        }
        try{
                int low = 0, high = pg.getTuples().size() - 1, mid = (low + high) / 2;int com=-1;
                while (low <= high) {
                    mid = (low + high) / 2;
                    Row tempTup = pg.getTuples().get(mid);
                    Object midVal = tempTup.getValue(pk);
                    com= comparePKs(strClusteringKeyValue, midVal);
                    if (com == 0 && isEqual(htblColNameValue, tempTup)) {
                        deleteRow(mid, pg);
                        break;
                    }
                    if (com < 0)
                        high = mid - 1;
                    else
                        low = mid + 1;
    
                }
                if(com==0)
                    deleteBTree(htblColNameValue, strClusteringKeyValue);
    
          
        }
        catch(DBAppException e){
            throw new DBAppException("This Tuple isn't avaliable in this table");

        }
      


    }

    public boolean isEqual(Hashtable<String, Object> htblColNameValue, Row row) {
        Hashtable<String, Object> temp = row.getData();

        if (htblColNameValue.size() != temp.size()) {
            return false;
        }

        for (String key : htblColNameValue.keySet()) {
            if (!temp.containsKey(key)) {
                return false;
            }

            Object value1 = htblColNameValue.get(key);
            Object value2 = temp.get(key);

            if (value1 == null) {
                if (value2 != null) {
                    return false;
                }
            } else {
                if (!value1.equals(value2)) {
                    return false;
                }
            }
        }

        return true;
    }

    public void deleteBTree(Hashtable<String, Object> htblColNameValue, String strClusteringKeyValue)
            throws DBAppException {
        for (Object key : indexNames.keySet()) {
            String indexTableName = indexNames.get(key);
            BPlusTree t = getTree(indexTableName);
            Vector<Row> returned = new Vector<>();
            Object k = htblColNameValue.get(key);
            if (k instanceof String) {
                String k0 = (String) k.toString();
                returned = (Vector<Row>) t.search(k0);
            } else if (k instanceof Double) {
                Double k1 = (Double) k;
                returned = (Vector<Row>) t.search(k1);
            } else if (k instanceof Integer) {
                Integer k2 = (Integer) k;
                returned = (Vector<Row>) t.search(k2);
            } else {
                throw new DBAppException("Unsupported key type");
            }
            String pk = DBApp.getClusterKey(this.name);
            if (returned.size() >= 2) {
                for (Row r : returned) {
                    if (comparePKs((String) htblColNameValue.get(pk.trim()).toString(),
                            (r.getData().get(pk.trim()))) == 0) {
                        returned.remove(r);
                        Object z = k;
                        if (z instanceof String) {
                            String k8 = (String) z.toString();
                            t.insert(k8, returned);
                        } else if (z instanceof Double) {
                            Double k6 = (Double) z;
                            t.insert(k6, returned);
                        } else if (z instanceof Integer) {
                            Integer k7 = (Integer) z;
                            t.insert(k7, returned);
                        } else {
                            throw new DBAppException("Unsupported key type");
                        }
                    }
                }
            }

            else {
                Object y = htblColNameValue.get(key);
                if (y instanceof String) {
                    String k0 = (String) y.toString();

                    t.delete(k0);
                } else if (y instanceof Double) {
                    Double k1 = (Double) y;
                    t.delete(k1);
                } else if (y instanceof Integer) {
                    Integer k2 = (Integer) y;
                    t.delete(k2);
                } else {
                    throw new DBAppException("Unsupported key type");
                }
            }
            saveTree(t, indexTableName);

        }
    }

    public Page findPage(String strClusteringKeyValue) throws DBAppException {
        String pk = DBApp.getClusterKey(this.name).trim();
        for (int i = 0; i < pageNums.size(); i++) {
            Page pg = getPageByNumber(pageNums.get(i));
            Object val0 = pg.getTuples().get(0).getData().get(pk);

            Object val1 = pg.getTuples().get(pg.getTuples().size() - 1).getData().get(pk);
            if (comparePKs(strClusteringKeyValue, val0) >= 0 && comparePKs(strClusteringKeyValue, val1) <= 0) {
                return pg;
            }
        }
        return null;

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table name: " + this.name + "\n");
        sb.append(String.format("-----------------------%s------------------------- \n", this.name));
        for (Integer i : this.pageNums) {
            try {
                sb.append(getPageByNumber(i) + "\n");
            } catch (Exception e) {
                try {
                    throw new DBAppException(e);
                } catch (DBAppException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return sb.toString();
    }

    public void indexing(String strColName, String strIndexName) throws DBAppException {
        if (indexNames.contains(strIndexName)) {
            throw new DBAppException("Index already Exists");
        }
        BPlusTree myTree = new BPlusTree<>(PAGE_SIZE - 1);

        Hashtable<Object, Vector<Row>> rows = new Hashtable<>();
        for (int i = 0; i < pageNums.size(); i++) {
            Page p = getPageByNumber(i + 1);

            for (Row r : p.getTuples()) {
                Object colValue = r.getValue(strColName);

                if (rows.containsKey(colValue)) {
                    rows.get(colValue).add(r);
                } else {
                    Vector<Row> newRowVector = new Vector<>();
                    newRowVector.add(r);
                    rows.put(colValue, newRowVector);
                }
            }
            savePage(p);

        }
        for (Object key : rows.keySet()) {
            Vector<Row> rowVector = rows.get(key);
            myTree.insert((Comparable) key, rowVector);

        }

        saveTree(myTree, strIndexName);
        indexNames.put(strColName, strIndexName);
        System.out.println("Index Created Successfully");
    }

    public void saveTree(BPlusTree b, String strIndexName) throws DBAppException {
        String filePath = "Index/" + strIndexName + ".class";
        try (FileOutputStream fileOut = new FileOutputStream(filePath);
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(b);
            out.flush();
            out.close();

        } catch (IOException e) {
            throw new DBAppException(e);
        }
    }

    public static void main(String[] args) throws DBAppException {
        // Table t =new Table("aa");
        // Hashtable <String, Object> x=new Hashtable<>();
        // x.put("id", new Integer(0));
        // x.put("name", new String("Ahmed"));
        // x.put("gpa", new Double(5.9));
        // Row r1 = new Row(x,"aa");
        // / try {
        // // t.createPage();
        // // t.getPageByNumber(1).insertTuple(r1);
        // System.out.println(comparePKs("1222", new String("1222")));
        // String filePath = "pages.ser";

        // // Create a File object with the specified file path
        // File file = new File(filePath);

        // // Check if the file name ends with ".ser"
        // if (file.getName().endsWith(".ser")) {
        // System.out.println("The file has the .ser extension.");
        // } else {
        // System.out.println("The file does not have the .ser extension.");
        // }

        // } catch (DBAppException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // }
        // try {
        // Page p = null;
        // FileInputStream fileIn = new FileInputStream("Pages/" + "Student" + "" + 2 +
        // ".class");
        // ObjectInputStream in = new ObjectInputStream(fileIn);
        // p = (Page) in.readObject();
        // in.close();
        // fileIn.close();
        // System.out.println(p);
        // }
        // catch (Exception e){
        // throw new DBAppException(e);
        // }
        // Hashtable<String,String> indexNames=new Hashtable<>();

        // System.out.println(indexNames.size());
        // String path = "Pages/" + "Student" + "" + 3 + ".class";
        // File file = new File(path);
        // file.delete();
        BPlusTree bp = getTree("gpaIndex");
        System.out.println(bp.search(1.9));
    }
}
