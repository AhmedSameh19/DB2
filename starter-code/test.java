import java.util.Hashtable;

public class test {
    public static void main(String[] args) {
        BPlusTree x= new BPlusTree<>();
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

			htblColNameValue.put("id", Integer.valueOf(2));
			htblColNameValue.put("name", "Ahmed Noor");
			htblColNameValue.put("gpa", Double.valueOf(0.23));
        Object r = 1;
            x.insert(1.2, (Comparable)r);
        // x.insert(1.3, 2);
        // x.insert(1.4, 7);
        // x.insert(1.5,8);
        // x.insert(1.6,3);
        // x.insert(1.7, 4);
        // x.insert(1.8, 5);
        // x.insert(1.9, 6);
        // x.insert(1.1, 7);
        // x.insert(1.0, 8);
        // x.insert(0.9,97);
        // x.insert(0.8, 17);
        // x.insert(0.8, 710);
        // x.delete(1.8);
        // x.delete(1.3);

        System.out.println((x.search(1.2)));




    }
}
