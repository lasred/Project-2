
public interface Connection {

	
	boolean createConnection();
	
	boolean send(byte[] data);
	
	byte[] receive();
}
