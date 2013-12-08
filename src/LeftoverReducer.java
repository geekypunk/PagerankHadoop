import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    // This is the alpha that controls the "random jump" or our web surfer.
    // See equation 5.1 of the chapter on PageRank
	
    public static double alpha = 0.85;
    public static int counterAmplifyFactor = 100000;
    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
    	
    	double leftOver = Double.parseDouble(context.getConfiguration().get("leftover"))/(double)counterAmplifyFactor;
    	
    	int size = Integer.parseInt(context.getConfiguration().get("size"));
    	
    	for(Node node: Ns){
    		double pgNew = 0;
    		pgNew+=alpha/(double)size;
    		pgNew+=(1-alpha)*((leftOver/(double)size)+node.getPageRank());
    		node.setPageRank(pgNew);
    		nid.set(node.nodeid);
    		context.write(nid, node);
    	}
    	
    }
}
