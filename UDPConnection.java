import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class UDPConnection implements Connection{
	
	private InetAddress inet;
	private int portNo;
	private DatagramSocket socket;
	DatagramPacket packet;
	public UDPConnection(InetAddress inet, int portNo){
		
		this.inet = inet;
		this.portNo = portNo;
	}
	@Override
	public boolean createConnection() {
		// TODO Auto-generated method stub
		try {
			socket = new DatagramSocket();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean send(byte[] data) {
		// TODO Auto-generated method stub
		try{
			packet = new DatagramPacket(data, data.length, inet, portNo);
				socket.send(packet);
		}catch(Exception e){
			return false;
		}
		return true;
		
	}

	@Override
	public byte[] receive() {
		// TODO Auto-generated method stub
		
		try{
		byte[] data = new byte[256];
		packet = new DatagramPacket(data, data.length);
		socket.receive(packet);
		return packet.getData();
		}catch(Exception e){
			return null;
		}
		
	}


}
