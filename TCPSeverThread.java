import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.InetAddress;
import java.util.*;
public class TCPSeverThread implements Runnable{
   private static Map<String, Connection> ipAddressToConnection;   
   private static final String[] ipAddressesOfAllMachines = 
             {"172.22.71.28","172.22.71.29", "172.22.71.30",
             "172.22.71.31", "172.22.71.32"}; 
    private static final int portOfOtherMachine = 7000;
    private static final String ACKNOWLEDGE_THIS = "Acknowledge"; 
    private Socket socket = null;
	
    public TCPSeverThread(Socket socket) {
        //only initialize these connections once
        ipAddressToConnection = new HashMap<String, Connection>();
        try {
            final String localIpAddress = InetAddress.getLocalHost().getHostAddress();
            for(String ipAddressOfMachine: ipAddressesOfAllMachines) {
                if(!localIpAddress.equals(ipAddressOfMachine)) {
                  Connection connection = new TCPConnection(ipAddressOfMachine, portOfOtherMachine);
                  connection.createConnection();
                  ipAddressToConnection.put(ipAddressOfMachine, connection);
                }
             }
        } catch(Exception e) {

        }
        this.socket =  socket;
    }

	@Override
	public void run() {	
		try{
			ServerEncoderDecoder encodeco = new ServerEncoderDecoder();
			PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
	        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        String inputLine;
	        while((inputLine = input.readLine()) != null){
                    //so you dont keep passing dat aback and forth 
                   if(!inputLine.contains(ACKNOWLEDGE_THIS)) {
                     for(final String ipAddress: ipAddressToConnection.keySet()) {
                        ipAddressToConnection.get(ipAddress).send((inputLine+ACKNOWLEDGE_THIS).getBytes());                      
                      }
                   }
                   
                Packet pac = encodeco.decodeData(inputLine.getBytes());
                Packet response = new Packet();
                if(pac.getOperation() == 1){
                	String key = pac.getKey();
                	if(Server.keyValueMap.get(key) != null){
                		response.setOperation((short) 5);
                		response.setValue(Server.keyValueMap.get(key));
                	}else{
                		response.setOperation((short) 6);
                	}
                }else if(pac.getOperation() == 2){
                	synchronized(Server.keyValueMap){
                		Server.keyValueMap.put(pac.getKey(), pac.getvalue());
                	}
                	response.setOperation((short) 4);
                }else if(pac.getOperation() == 3){
                	synchronized (Server.keyValueMap) {
                		Server.keyValueMap.remove(pac.getKey());
					}
                	response.setOperation((short) 7);
                }else{
                	System.out.println("Unknown operation requested");
                	response.setOperation((short) -1);
                }
                
                byte[] responseByte = encodeco.encodeData(response);
                output.println(new String(responseByte) + this.socket.getInetAddress().toString());
	        }
		}catch(Exception e){
			
		}
		
		
	}

}
