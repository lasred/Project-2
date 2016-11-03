import java.nio.ByteBuffer;

public class ServerEncoderDecoder {
	
	
	public byte[] encodeData(Packet p){
		byte[] encodedData = null;
		ByteBuffer data = ByteBuffer.allocate(1000);
		short operation = p.getOperation();
		data.putShort(operation);
		if(operation == 4 || operation == 6 || operation == 7){ // GET operation
			data.flip();
			return data.array();
			//return encodedData;
		}else if(operation == 5){ // PUT operation
			byte[] value = p.getvalue().getBytes();
			data.putInt(value.length);
			data.put(value);
			data.flip();
			return data.array();
		}else if(operation == -1){
			data.flip();
			return data.array();
		}
		return encodedData;
		
	}
	
	public Packet decodeData(byte[] data){
		
		Packet pac = new Packet();
		ByteBuffer response = ByteBuffer.wrap(data);
		short operation = response.getShort();
		if(operation == 1){ //operation successful
			pac.setOperation(operation);
			int keySize = response.getInt();
			byte[] keyBytes = new byte[keySize];
			response.get(keyBytes);
			pac.setKey(new String(keyBytes));
		}else if(operation == 2){ // value of key
			pac.setOperation(operation);
			int keySize = response.getInt();
			byte[] keyByte = new byte[keySize];
			response.get(keyByte);
			pac.setKey(new String(keyByte));
			int valueSize = response.getInt();
			byte[] valueByte = new byte[valueSize];
			response.get(valueByte);
			pac.setValue(new String(valueByte));
		} else if(operation == 3){
			pac.setOperation(operation);
			int keySize = response.getInt();
			byte[] keyByte = new byte[keySize];
			response.get(keyByte);
			pac.setKey(new String(keyByte));
		}else{
			System.out.println("Incorrect operation request");
			return null;
		}
		return pac;
	}

}
