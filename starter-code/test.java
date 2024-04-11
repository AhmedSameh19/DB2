import java.io.*;

public class test {
    public static void main(String[] args) throws DBAppException {
        try {
            BPlusTree p = null;
            FileInputStream fileIn = new FileInputStream("Index/" + "idIndex" + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (BPlusTree) in.readObject();
            in.close();
            fileIn.close();
            System.out.println(p.search(2343432));
        }
        catch (Exception e){
            throw new DBAppException(e);
        }

    }
}
