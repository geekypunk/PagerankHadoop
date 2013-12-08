import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PageRank {

    public static void main(String[] args) throws IOException {
		
    	int numRepititions = 5; //The number of PageRank passes to run
		long leftover = 0; //How much PageRank mass did not get moved to any node
		
		/*
		 * This function returns the size of the input graph
		 * The size of the graph is determined by the number of lines in the input file.
		 * Also I assume that there will be only 1 input file(.txt as extension)at a time,
		 *  in the input folder,
		 * 		 
		 * */
		long size = getGraphSize("input"); // The size of the internet graph
		/*
		for(int i = 0; i < 2*numRepititions; i++) { 
			FileUtils.deleteDirectory(new File("stage"+i));
		}*/
		
		for(int i = 0; i < 2*numRepititions; i++) { //We need to run 2 iterations to make 1 pass
		    Job job;
		    //Run the right job for the current pass
		    if(i%2 == 0) {
		    	job = getTrustJob();
		    }
		    else {
		    	job = getLeftoverJob(leftover, size);
		    }
		    
		    String inputPath = i == 0 ? "input" : "stage" + (i-1);
		    String outputPath = "stage" + i;
	
		    FileInputFormat.addInputPath(job, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	
		    try { 
		    	job.waitForCompletion(true); //run the job
		    } catch(Exception e) {
		    	System.err.println("ERROR IN JOB: " + e);
		    	return;
		    }
		    if(i%2 == 0) {
		    	
		    	//Set the accumulated lost pagerank mass
		    	leftover = job.getCounters().findCounter(MyCounter.Counter).getValue();
		    	
		    }
		}
    }
    public static Job getStandardJob(String l, String s) throws IOException {
		Configuration conf = new Configuration();
		if(!l.equals("") && !s.equals("")) { //if we're in the Leftover job case
		    conf.set("leftover", l);         //note that we need to do this here since we don't have access to the configuration elsewhere
		    conf.set("size", s);
		}
		Job job = new Job(conf);
	
		job.setOutputKeyClass(IntWritable.class); //We output <Int, Node> pairs
		job.setOutputValueClass(Node.class);
	
		job.setInputFormatClass(NodeInputFormat.class); //We take in <Int,Node> pairs
		job.setOutputFormatClass(NodeOutputFormat.class);
	
		job.setJarByClass(PageRank.class); //The current jar we're in
	
		return job;
    }

    public static Job getTrustJob() throws IOException{

		Job job = getStandardJob("", ""); //We don't need any extra variables
	
		job.setMapOutputKeyClass(IntWritable.class); //Our mapper puts out something different than our reducer
		job.setMapOutputValueClass(NodeOrDouble.class); //in particular, we output <Int, Node+Double> pairs
		
		job.setMapperClass(TrustMapper.class);
		job.setReducerClass(TrustReducer.class);
	
		return job;
    }

    public static Job getLeftoverJob(long l, long s) throws IOException{
		
    	Job job = getStandardJob("" + l, "" + s);
	
		job.setMapperClass(LeftoverMapper.class);
		job.setReducerClass(LeftoverReducer.class);

		return job;
    }
    //Get files with .txt as extension
    public static File[] finder( String dirName){
    	File dir = new File(dirName);

    	return dir.listFiles(new FilenameFilter() { 
    	         public boolean accept(File dir, String filename)
    	              { return filename.endsWith(".txt"); }
    	} );

    }
    
    public static int getGraphSize(String inputFile) throws IOException{
    	File[] listOfFiles = finder(inputFile);
    	//Get the number of lines in the first(only) file present
    	InputStream is = new BufferedInputStream(new FileInputStream(listOfFiles[0]));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    	
    	
    }
}
	       

    


