
import java.io.Serializable;
import java.util.*;

public class Page implements Serializable {
    private Vector<Row> row;
    private int pageNumber;

    public void insertTuple(Row tuple) {
        row.add(tuple);
    }




    public void setRow(Vector<Row> row) {
        this.row = row;
    }


    public Page(int pageNumber) {
        this.pageNumber = pageNumber;
        row = new Vector<>();
    }


    public int getPageNumber() {
        return pageNumber;
    }

    public Vector<Row> getTuples() {
        return row;
    }
    public int size(){
        return row.size();
    }
    public String toString(){
        StringBuilder sb=new StringBuilder("");
        sb.append("Page "+getPageNumber()+"\n");
        for(Row r: row){
            sb.append(r+"\n");
        }
        return sb.toString() ;
    }
    // public static void main(String[] args) {
    //     Page p=new Page(2);
    //     Hashtable <String, Object> x=new Hashtable<>();
    //         x.put("id", new Integer(0));
    //         x.put("name", new String("Ahmed"));
    //         x.put("gpa", new Double(5.9));
    //         Row  r1 = new Row(x,"Student");
    //     p.insertTuple(r1);
    //     System.out.println(p);
    // }
}

