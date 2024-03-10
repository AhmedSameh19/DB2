import java.util.Hashtable;

public class Row  {
    private Hashtable<String,Object> data;
    private String tableName;
    
    public Row(Hashtable<String,Object> data,String tableName){
        this.tableName=tableName;
        this.data=data;

    }
    public Hashtable<String, Object> getData() {
        return data;
    }
    public void setData(Hashtable<String, Object> data) {
        this.data = data;
    }
    public Object getValue(String key) {
        return data.get(key);
    }
    public void setValue(String column, Object value) {
        data.replace(column , value);
    }

}
