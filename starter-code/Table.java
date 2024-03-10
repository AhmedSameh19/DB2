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
            int num =this.pageNums.get(this.pageNums.size()-1);
            if(getPageByNumber(num).getTuples().size()<Table.PAGE_SIZE){
                getPageByNumber(num).getTuples().add(record);
                savePage(getPageByNumber(num));
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
                        Page prevPage = getPageByNumber(i-1);
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
            if (p.getTuples().get(p.getTuples().size() - 1).compareTo(t) >= 0) {
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
   
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.name+"\n");
        sb.append("----------------------------------------");
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
    // public static void main(String[] args) {
    //     Table t =new Table("aa");
    //     Hashtable <String, Object> x=new Hashtable<>();
    //     x.put("id", new Integer(0));
    //     x.put("name", new String("Ahmed"));
    //     x.put("gpa", new Double(5.9));
    //     Row  r1 = new Row(x,"aa");
    //     try {
    //         t.createPage();
    //         t.getPageByNumber(1).insertTuple(r1);
    //         System.out.println(t.getPageByNumber(1).getTuples());
    //     } catch (DBAppException e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //     }
    // }
   
    
}
