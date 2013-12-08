
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class NodeInputFormat extends FileInputFormat<IntWritable, Node> {
    public RecordReader<IntWritable, Node> createRecordReader(InputSplit s, TaskAttemptContext ctx) throws IOException {
    	return new NodeRecordReader(); //Simply construct and return a NodeRecordReader
    }
}