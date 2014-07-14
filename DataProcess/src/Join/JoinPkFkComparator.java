package Join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class JoinPkFkComparator extends WritableComparator {
	public JoinPkFkComparator(){
		super(JoinRecordKey.class);
	}
	@Override
	 public int compare(WritableComparable a, WritableComparable b) {
		JoinRecordKey key1 = (JoinRecordKey)a;
		JoinRecordKey key2 = (JoinRecordKey)b;
		 //System.out.println("call compare");
		return key1.keyId.compareTo(key2.keyId);
	 }
}
