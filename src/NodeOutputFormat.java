import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NodeOutputFormat extends FileOutputFormat<IntWritable, Node> {
    
    //Describes how to get a NodeRecordWriter object.
    //Note the use of the Factory pattern
    public RecordWriter<IntWritable, Node> getRecordWriter(TaskAttemptContext ctxt) throws IOException, InterruptedException {
		Path file = FileOutputFormat.getOutputPath(new JobContext(ctxt.getConfiguration(), ctxt.getJobID())); //Get the path of the directory we're supposed to be writing to.
		file = new Path(file.toString() + "/output.txt"); //Find the Path of the file we're supposed to write to
		FileSystem fs = FileSystem.get(ctxt.getConfiguration()); //Create a filesystem object for HDFS
		FSDataOutputStream fileOut = fs.create(file); //And get a output stream for our file!
		return new NodeRecordWriter(fileOut); // Now we can use that stream for our writer
    }

}