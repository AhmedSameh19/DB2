import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

public class Row  implements Serializable{
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
    public int compareTo(Object o) throws DBAppException {
        String pk = DBApp.getClusterKey(this.tableName).trim();
        Object pkVal = data.get(pk);
        Row t = (Row) o;
        if (pkVal instanceof String)
            return ((String) pkVal).compareTo((String) t.getData().get(pk.trim()));
        else if (pkVal instanceof Integer)
            return (Integer) pkVal - (Integer) t.getData().get(pk.trim());
        else
            return ((Double) pkVal).compareTo((Double) t.getData().get(pk.trim()));
    }
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
        }

        // Remove the trailing comma and space if there are entries
        if (!data.isEmpty()) {
            sb.delete(sb.length() - 2, sb.length());
        }

        sb.append("}");

        return sb.toString();
    }
    // public static void main(String[] args) throws DBAppException {
    //     Hashtable <String, Object> x=new Hashtable<>();
    //     x.put("id", new Integer(0));
    //     x.put("name", new String("Ahmed"));
    //     x.put("gpa", new Double(5.9));
    //     Row  r1 = new Row(x,"Student");
    //     Hashtable <String, Object> z=new Hashtable<>();
    //     z.put("id", new Integer(1));
    //     z.put("name", new String("Sakr"));
    //     z.put("gpa", new Double(2.2));
    //     Row  r2 = new Row(z,"Student");
    //     System.out.println( r1);
    //     System.out.println("sakr".compareTo("ahmed"));
    // }
}
