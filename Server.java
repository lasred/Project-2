import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

public class Server {
	
	public static Map<String,String> keyValueMap = new HashMap<String, String>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length != 2){
			System.out.println("Incorrect no of arguments");
			System.exit(1);
		}
		String mode = args[0];
		int portNo = Integer.parseInt(args[1]);
		if("TCP".equalsIgnoreCase(mode)){
			try{
				ServerSocket serverSocket = new ServerSocket(portNo);
				while(true){
					Socket clientSocket = serverSocket.accept();
					TCPSeverThread tcpServerThread = new TCPSeverThread(clientSocket);
					Thread thread = new Thread(tcpServerThread);
					thread.start();
				}
			}catch(Exception e){
				
			}
		}else if("UDP".equalsIgnoreCase(mode)){
			
			try {
				UDPServerThread udpServer = new UDPServerThread(portNo);
				Thread thread = new Thread(udpServer);
				thread.start();
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}else if("RPC".equalsIgnoreCase(mode)){
			
		}else{
			System.out.println("Incorrect mode entered");
			System.exit(1);
		}
		
	}

}
 