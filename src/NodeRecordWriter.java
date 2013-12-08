import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class NodeRecordWriter extends RecordWriter<IntWritable, Node> {

    private DataOutputStream out; 

    public NodeRecordWriter(DataOutputStream out) throws IOException{
    	this.out = out;
    }

    public synchronized void write(IntWritable key, Node value) throws IOException {
	    boolean keyNull   = key == null;
	    boolean valueNull = value == null;
	    
	    if(valueNull) { //Can't write a Null value
	    	return;
	    }
	    if(keyNull) {
	    	write(new IntWritable(value.nodeid), value); //If we have a null key, then just use the Node's nodeid
	    }

	    String nodeRep = key.toString() + " " + value.getPageRank() + " "; //We can just output the nodeid and PageRank space-seperated
	    String outGoing = "";
	    for(Integer n : value) {//for outgoing edges, we need to comma-seperate them
	    	outGoing += n.toString() + ",";
	    }
	    if(!outGoing.equals("")) 
	    	nodeRep += outGoing.substring(0,outGoing.length()-1); //If there are outgoing nodes, we need to get rid of the trailing comma
	    out.writeBytes(nodeRep.trim() + "\n");//And write out the resulting record!
    }

    public synchronized void close(TaskAttemptContext ctxt) throws IOException{
        out.close();
    }
}