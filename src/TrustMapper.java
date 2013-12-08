import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class TrustMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {
	
	//counterAmplifyFactor(10^5) is used as counters are 8 byte integers and pageranks are too small
	public static int counterAmplifyFactor = 100000;
    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {
	
    	
    	double pagerankPass = value.getPageRank()/(double)value.outgoingSize();
    	key.set(value.nodeid);
    	context.write(key, new NodeOrDouble(value));
    	Iterator<Integer> outLinks = value.iterator();
    	Integer d;
    	//The below loop will no run for dangling nodes
    	while(outLinks.hasNext()){
    		d = outLinks.next();
    		key.set(d);
    		context.write(key, new NodeOrDouble(pagerankPass));
    	}
    	//Take care of dangling nodes
    	if(value.outgoingSize()==0){
    		double pgNew = value.getPageRank()*counterAmplifyFactor;
    		context.getCounter(MyCounter.Counter).increment((long)pgNew);
    	}
    
    }
}
