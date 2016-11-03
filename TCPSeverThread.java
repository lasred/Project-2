import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TCPSeverThread implements Runnable{

	private Socket socket = null;
	
    public TCPSeverThread(Socket socket) {

        this.socket =  socket;
    }

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			ServerEncoderDecoder encodeco = new ServerEncoderDecoder();
			PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
	        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        String inputLine;
	        while((inputLine = input.readLine()) != null){
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
                output.println(new String(responseByte));
	        }
		}catch(Exception e){
			
		}
		
		
	}

}
