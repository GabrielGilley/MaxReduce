import com.hazelcast.core.*;
import com.hazelcast.config.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Queue;
 
public class HazelcastServer {
	static int k = 31;
    public static void main(String[] args) {
    	try {
    		//System.out.println("Started");
	        Config cfg = new XmlConfigBuilder(args[0]).build();
	        //byte[] b = {65,67};
	        //String b = "ABCD";
	       
	        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
	        //Map<String, byte[]> map = instance.getMap("kmer");
	        //Map<String, String> map = instance.getMap("kmer");
	        
	        //map.put("1", b);
	        //map.put("TGAAATTAAGATCATTTATAACTGTAACTTTGGCACTGGGCATGATCGCAACGACTGGCGCTACTGTGGCAGGTATTGATTCATGGTACTGAAAGTCCGTA", "ArghyaTGAAATTAAGATCATTTATAACTGTAACTTTGGCACTGGGCATGATCGCAACGACTGGCGCTACTGTGGCAGGTATTGATTCATGGTACTGAAAGTCCGTA".getBytes());
	       
	        //System.out.println("Value = " + map.get("1"));
    	}
    	catch (Exception e) {e.printStackTrace();}
    }
}