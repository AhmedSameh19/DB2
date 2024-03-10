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
        else{
            int num =this.pageNums.get(this.pageNums.size()-1);
            if(getPageByNumber(num).size()<PAGE_SIZE){
                getPageByNumber(num).getTuples().add(record);
                savePage(getPageByNumber(num));
                return;
            }
            else{
                createPage();
                Page p = getPageByNumber(this.pageNums.get(this.pageNums.size()-1));
                p.getTuples().add(record);
                savePage(p);
                return;
            }
        }

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
    public void savePage(Page p) throws DBAppException{
        try {
            // Serialize the object to a file
            FileOutputStream fileOut = new FileOutputStream("Pages/" + this.name + "" + p.getPageNumber() + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
            p = null;
            System.gc();
        } catch(IOException e) {
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
}
