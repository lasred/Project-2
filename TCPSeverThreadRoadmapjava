import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TCPSeverThread implements Runnable{
	private Socket socket = null;
    private List<String> ipAddressOfOtherNodes;
    private String ACNOWLEDGE_THIS = "1st phase, acknowledge this";
    private String ACKNOWLEGED = "Acknowledged. First phase is done for this machine";
    private String GO_MESSAGE = "GO! Commit the transaction";
	private String GO_FINISHED_MESSAGE = "Transaction finished";

    public TCPSeverThread(Socket socket, List<String> ipAddressOfOtherNodes) {
        this.socket =  socket;
        this.ipAddressOfOtherNodes = ipAddressOfOtherNodes;
    }
    
    //somebody implement this?
    private void sendMessage(String message, Packet packet, String ipAddress) {

    }

    private void executeTransaction(Packet packet, PrintWriter output) {
        Packet response = new Packet();
         if(pac.getOperation() == 1){
            String key = pac.getKey();
            if(Server.keyValueMap.get(key) != null){
               response.setOperation((short) 5);
               response.setValue(Server.keyValueMap.get(key));
            }else{
               response.setOperation((short) 6);
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
            output.println(new String(responseByte));
        }
    }
    @Override
	public void run() {
		try{
			ServerEncoderDecoder encodeco = new ServerEncoderDecoder();
			PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
            //input stream could either come from the client or other serers 
	        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        String inputLine;
	        while((inputLine = input.readLine()) != null){
                Packet pac = encodeco.decodeData(inputLine.getBytes());
                //coming from the coordinator
                if(pac.getMessage().equals(ACNOWLEDGE_MESSAGE)) {
                    //dont do the transaction yet send the acknowledgement 
                    Packer buffer = pac;
                    sendMessage(ACKNOWLEGED, null, pac.getSenderAddress());
                    //stall until receiving a Go request
                    callback() {
                        executeTransaction(buffer);
                        sendMessage(GO_FINISHED_MESSAGE, null, pac.getSenderAddress());
                    }
                } else {
                    //coming from the client, we are coordinator \\
                    List<String> receivedNodes = new ArrayList<String>();
                    //act as coordinator 
                    for(String nodeIpAddress: ipAddressOfOtherNodes) {
                        //send out requests for 1st phase of 2 phase commit
                        //pac - buffer  
                        sendMessage(ACNOWLEDGE_MESSAGE, pac, nodeIpAddress);
                        //wait for acknowledgement 
                        while(1) {
                            callback.() {
                                //get message from nodeIpAddress
                                receivedNodes.add(nodeIpAddress);
                                //received acknowledgements from other nodes 
                                if(receivedNodes.containsAll(ipAddressOfOtherNodes)) {
                                    executeTransaction(pac);
                                    for(String nodeIpAddress: ipAddressOfOtherNodes) {
                                        sendMessage(GO_MESSAGE, nodeIpAddress);
                                    }
                                }
                            }
                        }
                    }
                }
	        }
		}catch(Exception e){
			
		}
		
		
	}

}
