
/** * @author Wael Abouelsaadat */ 
public class DBAppException extends Exception{
    public DBAppException() {
        super("NOT A VALID OPERATION");
    }
    public DBAppException(Exception e){
        super(e);
    }

    public DBAppException(String message) {
        super(message);
    }
}