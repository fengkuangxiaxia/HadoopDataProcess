import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
 



import java.io.IOException;
import java.util.Iterator;

public class HadoopDataProcessReducer extends MapReduceBase implements Reducer<RecordKey, Text, NullWritable, Text> {
	@Override
    public void reduce(RecordKey key, Iterator<Text> values, 
                    OutputCollector<NullWritable, Text> output, 
                    Reporter reporter) throws IOException {
		
		String sessionValue = "";	
		while (values.hasNext()) {	
			String[] tmp = values.next().toString().split("\t");
			try{
				if (tmp[1].equals("S")) {
					sessionValue = tmp[0];
				}
				else {
					//output.collect(NullWritable.get(), new Text(tmp[0] + "," + sessionValue));
					String[] temp = (tmp[0] + "," + sessionValue).split(",");
			        if(temp.length == 19){
			            String time = temp[0];
			            String gp = temp[1];
			            String site = temp[2];
			            String url = temp[3];
			            String Type = temp[4];
			            String size = temp[5];
			            String referer = temp[6];
			            String cip = temp[7];
			            int cport = Integer.valueOf(temp[8]);
			            String sip = temp[9];
			            int sport = Integer.valueOf(temp[10]);
			            String agent = temp[11];
			            int length = Integer.valueOf(temp[12]);
			            String parameterNumber = temp[13];
			            int hasAgent = Integer.valueOf(temp[14]);
			            int visitTime = Integer.valueOf(temp[15]);
			            int width = Integer.valueOf(temp[16]);
			            double getRate = Double.valueOf(temp[17]);
			            double staticPageRate = Double.valueOf(temp[18]);
			           
			            boolean isAttack = judge(cport, sport, length, hasAgent, visitTime, width, getRate, staticPageRate);
			            if (isAttack) {
			            	output.collect(NullWritable.get(), new Text(tmp[0] + "," + sessionValue));
			            }
			        }
				}
			}
			catch (Exception e) {
				;
			}			
		}
		
    }
	
	private boolean judge(int cport, int sport, int length, int hasAgent, int visitTime, int width, double getRate, double staticPageRate) {
		if (getRate <= 0.994413) {
			return true;
		}
		else {
			if (length <= 0) {
				return true;
			}
			else {
				if (width <= 31) {
					if (sport <= 80) {
						if (visitTime > 8) {
							return false;
						}
						else {
							if (width > 0) {
								return false;
							}
							else {
								if (hasAgent == 0) {
									return false;
								}
								else {
									if (visitTime <= 2) {
										if (cport <= 12388 || cport > 12750) {
											return false;
										}
										else {
											return true;
										}
									}
									else {
										if (visitTime > 4) {
											return false;
										}
										else {
											if (cport <= 12538 || cport > 12627) {
												return false;
											}
											else {
												return true;
											}
										}
									}
								}
							}
						}
					}
					else {
						if (cport > 2156) {
							return false;
						}
						else {
							if (visitTime > 29) {
								return false;
							}
							else {
								if (staticPageRate <= 0.5) {
									return true;
								}
								else {
									return false;
								}
							}
						}
					}
				}
				else {
					if (visitTime <= 180) {
						return true;
					}
					else {
						return false;
					}
				}
			}
		}
	}
}
