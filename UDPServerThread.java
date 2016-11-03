import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class UDPServerThread implements Runnable{

	public static DatagramSocket socket = null;
	ServerEncoderDecoder encodeco = new ServerEncoderDecoder();
	public static BlockingQueue<DatagramPacket> requestQueue = new LinkedBlockingQueue<DatagramPacket>();		
	public UDPServerThread(int portNo) throws SocketException {
		socket = new DatagramSocket(portNo);
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
		for(int i=0; i<10; i++){
			 RequestProcessorThread processorthread = new RequestProcessorThread();
             Thread thread = new Thread(processorthread, "processorthread" + i );
             pool.execute(thread);
		}
		while (true) {
            try {
                byte[] buffer = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                requestQueue.put(packet);
            }catch(Exception e){
            	e.printStackTrace();
            	break;
            }
		}
		 socket.close();
	}
	

}
