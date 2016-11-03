import java.nio.ByteBuffer;

public class ClientEncoderDecoder {

	
	public byte[] encodeData(Packet p){
		byte[] encodedData = null;
		ByteBuffer data = ByteBuffer.allocate(2000);
		short operation = p.getOperation();
		data.putShort(operation);
		if(operation == 1){ // GET operation
			byte[] key = p.getKey().getBytes();
			int keySize = key.length;
			data.putInt(keySize);
			data.put(key);
			data.flip();
			return data.array();
			//return encodedData;
		}else if(operation == 2){ // PUT operation
			byte[] key = p.getKey().getBytes();
			int keySize = key.length;
			data.putInt(keySize);
			data.put(key);
			byte[] value = p.getvalue().getBytes();
			data.putInt(value.length);
			data.put(value);
			data.flip();
			return data.array();
		}else if(operation == 3){
			byte[] key = p.getKey().getBytes();
			int keySize = key.length;
			data.putInt(keySize);
			data.put(key);
			data.flip();
			return data.array();
		}
		return encodedData;
		
	}
	
	public Packet decodeData(byte[] data){
		
		Packet pac = new Packet();
		ByteBuffer response = ByteBuffer.wrap(data);
		short operation = response.getShort();
		if(operation == 4){ //operation successful
			pac.setOperation(operation);
		}else if(operation == 5){ // value of key
			pac.setOperation(operation);
			int size = response.getInt();
			byte[] valueByte = new byte[size];
			response.get(valueByte);
			pac.setValue(new String(valueByte));
		}else if(operation == 6){ //operation successful
			pac.setOperation(operation);
		}else if(operation == 7){ //operation successful
			pac.setOperation(operation);
		}else if(operation == -1){
			System.out.println("Operation failed");
			return null;
		}
		return pac;
	}
}
