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
		return key1.keyId.compareTo(key2.keyId);
	 }
}
