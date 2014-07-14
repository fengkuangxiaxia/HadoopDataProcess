package Combine;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class CombinePkFkComparator extends WritableComparator {
	public CombinePkFkComparator(){
		super(CombineRecordKey.class);
	}
	@Override
	 public int compare(WritableComparable a, WritableComparable b) {
		CombineRecordKey key1 = (CombineRecordKey)a;
		CombineRecordKey key2 = (CombineRecordKey)b;
		 //System.out.println("call compare");
		return key1.keyId.compareTo(key2.keyId);
	 }
}
