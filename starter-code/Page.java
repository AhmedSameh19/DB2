
import java.util.*;

public class Page  {
    private Vector<Row> row;
    private int pageNumber;

    public void insertTuple(Row tuple) {
        row.add(tuple);
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
}

