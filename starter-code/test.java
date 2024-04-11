public class test {
    public static void main(String[] args) {
        BPlusTree x= new BPlusTree<>();
        x.insert(1.2, 1);
        x.insert(1.3, 2);
        x.insert(1.4, 7);
        x.insert(1.5,8);
        x.insert(1.6,3);
        x.insert(1.7, 4);
        x.insert(1.8, 5);
        x.insert(1.9, 6);
        x.insert(1.1, 7);
        x.insert(1.0, 8);
        x.insert(0.9,97);
        x.insert(0.8, 17);
        x.insert(0.8, 710);
        x.delete(1.8);
        x.delete(1.3);

        System.out.println(x);




    }
}
