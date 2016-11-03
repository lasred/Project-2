
public class Packet {

	private int sequenceNo;
	private short operation;
	private String key;
	private String value;
	
	public Packet(){
		
	}
	
	public Packet(String[] commandParts, int sequenceNo){
		this.sequenceNo = sequenceNo;
		if("GET".equalsIgnoreCase(commandParts[0])){
			operation = 1;
			key =  commandParts[1];
		}else if("PUT".equalsIgnoreCase(commandParts[0])){
			operation = 2;
			key = commandParts[1];
			value = commandParts[2];
		}else if("DELETE".equalsIgnoreCase(commandParts[0])){
			operation = 3;
			key = commandParts[1];
		}
	}
	public Packet(int sequenceNo, short operation, String key, String value){
		this.sequenceNo = sequenceNo;
		this.operation = operation;
		this.key = key;
		this.value = value;
				
	}
	
	public void setOperation(short operation){
		this.operation = operation;
	}
	
	public short getOperation(){
		return operation;
	}
	
	public int getSequenceNo(){
		return sequenceNo;
	}
	
	public void setSequenceNo(int sequenceNo){
		this.sequenceNo = sequenceNo;
	}
	
	public void setKey(String key){
		this.key = key;
	}
	
	public String getKey(){
		return key;
	}
	
	public void setValue(String value){
		this.value = value;
	}
	
	public String getvalue(){
		return value;
	}
}
