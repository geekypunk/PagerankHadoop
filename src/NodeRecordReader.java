import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

//Based off of the code at http://developer.yahoo.com/hadoop/tutorial/module5.html
public class NodeRecordReader extends RecordReader<IntWritable, Node> {
    private LineRecordReader lineReader; //Does the actual reading for us
    private LongWritable lineKey; // The key from lineReader
                                  // Contains the line number we're reading from
    private Text lineValue; // The value from lineReader
                            // Contains the text from the line we're reading
    private IntWritable curKey; // The current key of our NodeRecordReader, the current Node's nodeid
    private Node curVal; // The current value of our NodeRecordReader, the current Node
    //private long end = 0, start = 0, pos = 0, maxLineLength; //Line tracking variables


    public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
	//To initialize, create and initialize our lineReader
	lineReader = new LineRecordReader();
	lineReader.initialize(genericSplit, context); 
    }

    public boolean nextKeyValue() throws IOException {
	if(!lineReader.nextKeyValue()) {//If we're out of lines, we're done
	    return false;
	}
	
	//get the correct current key and value
	lineKey = lineReader.getCurrentKey();
	lineValue = lineReader.getCurrentValue();

	//The line needs to be split into 3 pieces: the nodeid, the current PageRank, and the optional list of outgoing links
	String[] pieces = lineValue.toString().split("\\s+");
	if(pieces.length < 2 || pieces.length > 3) {
	    throw new IOException("Given poorly-formatted record: " + lineValue.toString()); //If this isn't what's going on, then we need to report an error.
	}

	int[] outs; //Now, we need to determine the outgoing links.
	if(pieces.length == 3) { //If there are any
	    String[] outNodes = pieces[2].split(","); //Than we know that they're seperated by commas
	    outs = new int[outNodes.length];
	
	    for(int i = 0; i < outNodes.length; i++) {
		int n;
		try {
		    n = Integer.parseInt(outNodes[i].trim());//attempt to parse the outgoing nodeids
		} catch (NumberFormatException nfe) {
		    throw new IOException("Error parsing integer in record: " + nfe.toString());//if we can't, then throw an error
		}
		outs[i] = n;
	    }
	} else {//Otherwise, there aren't any, so we use a 0-size array
	    outs = new int[0];
	}
	
	int nodeId;//attempt to parse the nodeid
	try {
	    nodeId = Integer.parseInt(pieces[0]);
	} catch(NumberFormatException nfe) {
	    throw new IOException(" Error parsing integer in record: " + nfe.toString());
	}
	
	double pageRank;//attempt to parse the current PageRank
	try {
	    pageRank = Double.parseDouble(pieces[1]);
	} catch(NumberFormatException nfe) {
	    throw new IOException(" Error parsing double in record: " + nfe.toString());
	}

	//we now know we'll succeed
	curKey = new IntWritable(nodeId);
	curVal = new Node(nodeId, outs);
	curVal.setPageRank(pageRank);
	
	return true;
    }
		

    public IntWritable getCurrentKey() {
	return curKey;
    }

    public Node getCurrentValue() {
	return curVal;
    }
    
    public void close() throws IOException {
	lineReader.close();
    }

    public float getProgress() throws IOException {
	return lineReader.getProgress();
    }
}