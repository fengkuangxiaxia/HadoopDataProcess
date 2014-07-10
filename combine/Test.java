package combine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class Test {
	
	public static List<String[]> Request = new ArrayList<String[]>();
	
	public static int linenum = 0;
	
	public static int request = 0;
	
	public static int response = 0;
	
	public static void main(String args[]){
		try{
			FileReader fr = new FileReader("C:\\Users\\wjbnys\\Desktop\\http-2way.txt");
			FileWriter fw = new FileWriter("C:\\Users\\wjbnys\\Desktop\\http-2way_result.txt");
			BufferedWriter bw = new BufferedWriter(fw);
	        BufferedReader br=new BufferedReader(fr);
	        String row;
	        while((row=br.readLine())!=null){
	            linenum++;
	            String[] elements = row.split("\\|");
	            if(elements.length == 12 || elements.length == 11){
	            	if(elements.length == 11){
	            		elements = (row + "|").split("\\|");
	            	}
		            if(elements[1].toUpperCase().equals("G") || elements[1].toUpperCase().equals("P")){
		            	Request.add(elements);
		            	request++;
		            } else {
		            	response++;
		            	String ServerIP = elements[7],ServerPort = elements[8], ClientIP = elements[9], ClientPort = elements[10];
		            	for(int i = 0 ; i < Request.size(); i++){
		            		String[] _temp = Request.get(i);
		            		if(_temp[7].equals(ClientIP) && _temp[8].equals(ClientPort) && _temp[9].equals(ServerIP) && _temp[10].equals(ServerPort)){
			            		StringBuffer sb=new StringBuffer();
			                    for(int j=0;j<_temp.length;j++){
			                    	sb.append(_temp[j]).append('|');
			                    }
			                    for(int j=0;j<elements.length;j++){
			                    	if(j<=10 && j>=7){
			                    		continue;
			                    	} else if(j==(elements.length-1)){
			                    		sb.append(elements[j]);
			                    	}else{
			                    		sb.append(elements[j]).append('|');
			                    	}
			                    }
			                    bw.write((new String(sb))+"\n");
			                    Request.remove(i);
			                    break;
		            		}
		            	}
		            }
	            } else {
	            	System.out.println("行" + linenum + "格式有误！" + elements.length);
	            }
	        }
	        br.close();bw.close();
	        System.out.println("请求数："+request+"\n回应数："+response+"\n请求未回应数："+Request.size());
		} catch(Exception e) {
			System.out.println(e.toString());
		}
	}
}
