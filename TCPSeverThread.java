import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import java.net.InetAddress;

public class TCPSeverThread implements Runnable{

    private static final String ACKNOWLEDGE_UPDATE = " Is update that needs to be acknowledged";
    private static final String UPDATE_ACKNOWLEDGED = "Update Acknowledged!";
    private static final String GO_MESSAGE = "GO";
    private static final String[] ipAddressesOfAllMachines = 
             {"172.22.71.28","172.22.71.29"}; 
    private static final int portOfOtherMachine = 7000;
    private static final int GET_OPERATION = 1;
    private static final int PUT_OPERATION = 2;
    private static final int DELETE_OPERATION = 3;

    private Socket socket;
	
    public TCPSeverThread(Socket socket) {
        this.socket =  socket;
    }

	@Override
	public void run() {	
		try{
			ServerEncoderDecoder encodeco = new ServerEncoderDecoder();
			PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
	        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        String inputLine;
            Map<String, Connection> ipAddressToConnection = null;
            Packet buffer = null;
            final String localIpAddress = InetAddress.getLocalHost().getHostAddress();
            final Set<String> acknowledgers = new HashSet<String>();
	        while((inputLine = input.readLine()) != null){
                 if(ipAddressToConnection == null) {
                     ipAddressToConnection = new HashMap<String, Connection>();
                     for(String ipAddressOfMachine: ipAddressesOfAllMachines) {
                        if(!localIpAddress.equals(ipAddressOfMachine)) {
                           try{
                              Connection connection = new TCPConnection(ipAddressOfMachine, portOfOtherMachine); 
                              connection.createConnection();
                              ipAddressToConnection.put(ipAddressOfMachine, connection);
                           }catch(Exception e){
                        
                           }
                       }
                     }
                 }
                System.out.println(inputLine);
                Packet pac = encodeco.decodeData(inputLine.getBytes());
                 //only forward non acknowledgements, only need to do this for PUT or
                //
                 //DELETE operations
                if(!inputLine.contains(ACKNOWLEDGE_UPDATE) && pac.getOperation() != GET_OPERATION) {
                  System.out.println("Forwarded Correctly");
                  for(final String ipAddress: ipAddressToConnection.keySet()) {
                     //first part of two phase commit 
                    ipAddressToConnection.get(ipAddress).send((inputLine+ACKNOWLEDGE_UPDATE).getBytes());                      
                  }
                  //everything is good till here
                }
                else if(inputLine.contains(ACKNOWLEDGE_UPDATE)) {
                    buffer = pac;
                    System.out.println("acknowledge update, hit coorindator back");
                    output.println(inputLine + UPDATE_ACKNOWLEDGED);
                } else if(inputLine.contains(UPDATE_ACKNOWLEDGED)) {
                    //once you get all of them back, then GO MESSAGE
                    String acknowledgerIpAddress = this.socket.getRemoteSocketAddress().toString();
                    System.out.println("acknowledged ip address " + acknowledgerIpAddress);
                    acknowledgers.add(acknowledgerIpAddress);
                    Set<String> allMachinesThatNeedToSendAcknowledgement = ipAddressToConnection.keySet();
                    if(acknowledgers.equals(allMachinesThatNeedToSendAcknowledgement)) {
                        acknowledgers.clear();
                        //once you get all of them back, then GO MESSAGE
                        for(final String ipAddress: ipAddressToConnection.keySet()) {
                           ipAddressToConnection.get(ipAddress).send((GO_MESSAGE+inputLine).getBytes());                      
                        }
                        //execute the transaction as well
                        executeTransaction(pac, output, encodeco);
                    }
                }  else if(inputLine.contains(GO_MESSAGE)) {
                    executeTransaction(buffer, output, encodeco);
                } else if(pac.getOperation() == GET_OPERATION) {
                    executeTransaction(pac, output, encodeco);
                }
          }
        }catch(Exception e){
			
		}
	}


    private static void executeTransaction(Packet pac, PrintWriter output, ServerEncoderDecoder encodeco) {
        Packet response = new Packet();
        if(pac.getOperation() == GET_OPERATION){
            String key = pac.getKey();
            if(Server.keyValueMap.get(key) != null){
                response.setOperation((short) 5);
                response.setValue(Server.keyValueMap.get(key));
            }else{
                response.setOperation((short) 6);
            }
        }else if(pac.getOperation() == PUT_OPERATION){
            synchronized(Server.keyValueMap){
                Server.keyValueMap.put(pac.getKey(), pac.getvalue());
            }
            response.setOperation((short) 4);
        }else if(pac.getOperation() == DELETE_OPERATION){
            synchronized (Server.keyValueMap) {
                Server.keyValueMap.remove(pac.getKey());
            }
            response.setOperation((short) 7);
        }else{
            System.out.println("Unknown operation requested");
            response.setOperation((short) -1);
        }
        byte[] responseByte = encodeco.encodeData(response);
        output.println(new String(responseByte));     
    }

}
