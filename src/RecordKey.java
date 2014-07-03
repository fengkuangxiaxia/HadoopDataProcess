import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class RecordKey implements WritableComparable<RecordKey>{
	String keyId;
	boolean isPrimary;

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.keyId = in.readUTF();
		this.isPrimary = in.readBoolean();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(keyId);
		out.writeBoolean(isPrimary);
	}
	
	public int compareTo(RecordKey k) {
		// TODO Auto-generated method stub
		if(this.keyId.equals(k.keyId)){
			if(k.isPrimary == this.isPrimary)
				return 0;
			return this.isPrimary? -1:1;
			
		}else
			return this.keyId.compareTo(k.keyId);
	}
	@Override
	public int hashCode() {
	    return this.keyId.hashCode();
	}
}
