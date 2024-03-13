import java.io.*;
import java.util.*;

    public class Table implements Serializable {
    static int PAGE_SIZE;
    String name;
    Vector<Integer> pageNums;

    public Table(String name) {
        this.name = name;
        this.pageNums = new Vector<>();
    }

    public void insertIntoTable(Row record) throws DBAppException{
        if(this.pageNums.size()==0){
            createPage();
            Page p = getPageByNumber(this.pageNums.get(0));
            p.getTuples().add(record);
            savePage(p);
            return;
        }
        Page p = getPageByTuple(record);

        if(p==null){
            Page p3 = getPageByNumber(this.pageNums.get(this.pageNums.size()-1));
            if(p3.size()<Table.PAGE_SIZE){
                p3.getTuples().add(record);
                savePage(p3);
                return;
            }
            createPage();
            Page p2 = getPageByNumber(this.pageNums.get(this.pageNums.size()-1));
            p2.getTuples().add(record);
            savePage(p2);
            return;
            }
            Row first = p.getTuples().get(0);
            if (record.compareTo(first) < 0){
                for (int i = 0 ; i < pageNums.size() ; i++){
                    int pgNum = pageNums.get(i);
                    if (pgNum == p.getPageNumber() && i!=0){
                        Page prevPage = getPageByNumber(pgNum-1);
                        if (prevPage.getTuples().size() < Table.PAGE_SIZE){
                            prevPage.getTuples().add(record);
                            savePage(prevPage);
                            return;
                        }
                    }
                }
            }
            this.insertRec(record, p);

        }

    
    private void insertRec(Row record, Page p) throws DBAppException {
        Vector<Row> tuples = p.getTuples();
        if (tuples.isEmpty()){
            tuples.add(record);
            p.setRow(tuples);
            savePage(p);
            return;
        }
        int low = 0 , high = tuples.size() - 1 , mid = (low + high) / 2;
        while (low <= high){
            mid = (low + high) / 2;
            Row tempTup = tuples.get(mid);
            int com = record.compareTo(tempTup);
            if (com == 0){
                throw new DBAppException("Existing PK");
            }
            if (com < 0)
                high = mid - 1;
            else
                low = mid + 1;
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
            for (int i = 0 ; i < pageNums.size() ; i++){
                int pgNum = pageNums.get(i);
                if (p.getPageNumber() == pgNum){
                    pageNum = pageNums.get(i+1);
                    break;
                }
            }
            Page nextPage = getPageByNumber(pageNum); // edit
            this.insertRec(tuples.get(tuples.size() - 1), nextPage);
            for (int i = tuples.size()-1 ; i > mid ; i--){
                tuples.set(i , tuples.get(i-1));
            }
            tuples.set(mid , record);
            p.setRow(tuples);
            savePage(p);
            return;
        }
        Row last = tuples.get(tuples.size() - 1);
        for (int i = tuples.size()-1 ; i > mid ; i--){
            tuples.set(i , tuples.get(i-1));
        }
        tuples.set(mid , record);
        tuples.add(last);
        p.setRow(tuples);
        savePage(p);  
      }

    public Page getPageByTuple(Row t) throws DBAppException {
        for (int i = 0 ; i < this.pageNums.size() ; i++){
            int pgNum = pageNums.get(i);
            Page p = getPageByNumber(pgNum);
            if (p.getTuples().get(p.getTuples().size() - 1).compareTo(t) > 0) {
//                if (p.getTuples().get(0).compareTo(t) > 0 && i != 0){
//                    Page prev = getPageByNumber(pageNums.get(i-1));
//                    if (prev.getTuples().size() < PAGE_SIZE)
//                        return prev;
//                }
                return p;
            }
        }
        return null; // bigger than largest record in last page
    }
    public void createPage() throws DBAppException{
        if (pageNums.size() == 0){
            Page p = new Page(1);
            this.pageNums.add(1);
            savePage(p);
            return;
        }
        Page p = new Page(pageNums.get(pageNums.size() - 1)+1);
        this.pageNums.add(p.getPageNumber());
        savePage(p);
    }
    public void savePage(Page p) throws DBAppException {
        String filePath = "Pages/" + this.name + "" + p.getPageNumber() + ".class";
        
        try (FileOutputStream fileOut = new FileOutputStream(filePath);
            ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(p);
            out.flush();    
            //closing the stream    
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
        }
        catch (Exception e){
            throw new DBAppException(e);
        }
    }
    public Page updateme(String strClusteringKeyValue) throws DBAppException{
        String pk = DBApp.getClusterKey(this.name).trim();
        for(int i = 0 ; i < pageNums.size() ; i++){
            Page pg = getPageByNumber(pageNums.get(i));
            Object val0 = pg.getTuples().get(0).getData().get(pk);

            Object val1 = pg.getTuples().get(pg.getTuples().size() - 1).getData().get(pk);
            if(comparePKs(strClusteringKeyValue,val0)>=0 && comparePKs(strClusteringKeyValue,val1)<=0) {
                return pg;
            }
        }
        return null;
    }
    public void updateTable(Row record,String strClusteringKeyValue) throws DBAppException {
        String pk = DBApp.getClusterKey(this.name).trim();
        Page pg=updateme(strClusteringKeyValue);

        if (pg == null || pk== null) {
            throw new DBAppException("DOESNT EXIST");
        }
        int low = 0, high = pg.getTuples().size() - 1, mid = (low + high) / 2;
        while (low <= high) {
            mid = (low + high) / 2;
            Row tempTup = pg.getTuples().get(mid);
            Object midVal = tempTup.getValue(pk);
            int com= comparePKs(strClusteringKeyValue,midVal);
            if (com== 0) {
                for (String str : record.getData().keySet()) {
                    tempTup.getData().replace(str, record.getData().get(str));
                }
                savePage(pg);
                return;
            }
            if (com < 0)
                high = mid - 1;
            else
                low = mid + 1;
        }
        throw new DBAppException("PK DOESNT EXIST");

    }
    public static int comparePKs(String stringPK , Object realPK) throws DBAppException {
        if (realPK instanceof String){
            return stringPK.compareTo((String) realPK);
        }
        else if (realPK instanceof Integer){
            Integer sPK = Integer.parseInt(stringPK);
            Integer rPK = (Integer) realPK;
            return sPK.compareTo(rPK);
        }
        else if (realPK instanceof Double){
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
            Page p = getPageByNumber(i+1);
            for (int j = 0; j < p.size(); j++) {
                Row r = p.getTuples().get(j);
                switch (_strOperator) {
                    case "=":
                        if (r.getValue(_strColumnName).equals(val)) {
                            res.add(r);
                        }
                        break;
                    case ">=":
                        if ((Double)r.getValue(_strColumnName) >= (Double)val) {
                            res.add(r);
                        }
                        break;
                    case ">":
                        if ((Double)r.getValue(_strColumnName) > (Double)val) {
                            res.add(r);
                        }
                        break;
                    case "<=":
                        if ((Double)r.getValue(_strColumnName) <= (Double)val) {
                            res.add(r);
                        }
                        break;
                    case "<":
                        if ((Double)r.getValue(_strColumnName) < (Double)val) {
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
        String[] operators = {">", ">=", "<", "<=", "!=", "="};
        for (String op : operators) {
            if (expression.contains(op)) {
                return op;
            }
        }
        return null;
    }
    public void deleteRecord(int index, Page p , boolean flag) throws DBAppException {

        p.getTuples().remove(index);
        if (p.getTuples().size() == 0){
            String path = "Pages/" + this.name + "" + p.getPageNumber() + ".class";
            File file = new File(path);
            System.out.println(file.delete());
            flag = false;
            for (int j = 0 ; j < pageNums.size() ; j++) {
                int num = pageNums.get(j);
                if (num==p.getPageNumber()) {
                    this.pageNums.remove(j);
                    break;
                }
            }
        }
        else {
            savePage(p);
        }
    }
    public void deleteRecord(int index, Page p) throws DBAppException {
        p.getTuples().remove(index);
        if (p.getTuples().size() == 0){
            String path = "Pages/" + this.name + "" + p.getPageNumber() + ".class";
            File file = new File(path);
            System.out.println(file.delete());
            for (int j = 0 ; j < pageNums.size() ; j++) {
                int num = pageNums.get(j);
                if (num==p.getPageNumber()) {
                    this.pageNums.remove(j);
                    break;
                }
            }
        }
        else {
            savePage(p);
        }
    }    
    public int findRec(Row record,String strClusteringKeyValue) throws DBAppException {
        
        String pk = DBApp.getClusterKey(this.name).trim();
        Page pg=findPage(strClusteringKeyValue);

        if (pk == null) {
            throw new DBAppException("PK DOESNT EXIST");
        }
        int low = 0, high = pg.getTuples().size() - 1, mid = (low + high) / 2;
        while (low <= high) {
            mid = (low + high) / 2;
            Row tempTup = pg.getTuples().get(mid);
            Object midVal = tempTup.getValue(pk);
            int com= comparePKs(strClusteringKeyValue,midVal);
            if (com== 0) 
                return mid;
            if (com < 0)
                high = mid - 1;
            else
                low = mid + 1;
        

    }
    return -1;

}

    public Page findPage (String strClusteringKeyValue) throws DBAppException{
        String pk = DBApp.getClusterKey(this.name).trim();
        for(int i = 0 ; i < pageNums.size() ; i++){
            Page pg = getPageByNumber(pageNums.get(i));
            Object val0 = pg.getTuples().get(0).getData().get(pk);

            Object val1 = pg.getTuples().get(pg.getTuples().size() - 1).getData().get(pk);
            if(comparePKs(strClusteringKeyValue,val0)>=0 && comparePKs(strClusteringKeyValue,val1)<=0) {
                return pg;
            }
        }
        return null;

    }
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.name+"\n");
        sb.append(String.format("-----------------------%s------------------------- \n",this.name)); 
            for(Integer i : this.pageNums){
            try {
                sb.append(getPageByNumber(i)+"\n");
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
     public static void main(String[] args) throws DBAppException {
    //     Table t =new Table("aa");
    //     Hashtable <String, Object> x=new Hashtable<>();
    //     x.put("id", new Integer(0));
    //     x.put("name", new String("Ahmed"));
    //     x.put("gpa", new Double(5.9));
    //     Row  r1 = new Row(x,"aa");
    // /     try {
    //         // t.createPage();
    //         // t.getPageByNumber(1).insertTuple(r1);
              System.out.println(comparePKs("1222", new String("1222")));
            
    //     } catch (DBAppException e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //     }
    // }
   
    
}}
