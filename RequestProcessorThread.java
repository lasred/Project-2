import java.net.DatagramPacket;
import java.net.InetAddress;

public class RequestProcessorThread implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		try{
			ServerEncoderDecoder encodeco = new ServerEncoderDecoder();
			while(true){
				DatagramPacket packet = UDPServerThread.requestQueue.take();
				byte[] inputPacket = packet.getData();
                Packet pac = encodeco.decodeData(inputPacket);
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
                InetAddress address = packet.getAddress();
                int port = packet.getPort();
                packet = new DatagramPacket(responseByte, responseByte.length, address, port);
                	UDPServerThread.socket.send(packet);
			}
		}catch(Exception e){
			
		}
	}

	
}
