import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TrustReducer extends Reducer<IntWritable, NodeOrDouble, IntWritable, Node> {
    public void reduce(IntWritable key, Iterable<NodeOrDouble> values, Context context)	throws IOException, InterruptedException {
    	Node M = null;
    	double sum=0;
    	for (NodeOrDouble val : values) {
    		
    		if(val.isNode()){
    			M = val.getNode();
    			//key.set(M.nodeid);
    			
    		}else{
    			sum+=val.getDouble();
    		}
    	
    	}
    	M.setPageRank(sum);
    	key.set(M.nodeid);
    	context.write(key, M);
    }
}
