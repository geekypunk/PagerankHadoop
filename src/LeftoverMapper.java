import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class LeftoverMapper extends Mapper<IntWritable, Node, IntWritable, Node> {

	//By symmetry, this mapper has been left empty, all the calculation is done in the reducer.
    public void map(IntWritable nid, Node N, Context context) throws IOException, InterruptedException {
    	
    	nid.set(N.nodeid);
    	context.write(nid,N);
    		
    }
}
