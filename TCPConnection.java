import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TCPConnection implements Connection{

	private String hostname;
	private int portNo;
	private Socket socket;
	BufferedReader input;
	PrintWriter output;
	public TCPConnection(String hostname, int portNo) {
		// TODO Auto-generated constructor stub
		this.hostname = hostname;
		this.portNo = portNo;
	}
	@Override
	public boolean createConnection() {
		// TODO Auto-generated method stub
		
		try{
			socket = new Socket(hostname, portNo);
			input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			output = new PrintWriter(socket.getOutputStream(), true);
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean send(byte[] data) {
		// TODO Auto-generated method stub
		try{
			output.println(new String(data));
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
		
	}

	@Override
	public byte[] receive() {
		// TODO Auto-generated method stub
		try{
			String inputMessage = input.readLine();
			return inputMessage.getBytes();
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
		
	}

}
