import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class PkFkComparator extends WritableComparator {
	public PkFkComparator(){
		super(RecordKey.class);
	}
	@Override
	 public int compare(WritableComparable a, WritableComparable b) {
		RecordKey key1 = (RecordKey)a;
		RecordKey key2 = (RecordKey)b;
		 //System.out.println("call compare");
		 if(key1.keyId.equals(key2.keyId)){
			return 0;	
		 }else
			return key1.keyId.compareTo(key2.keyId);
	 }
}
