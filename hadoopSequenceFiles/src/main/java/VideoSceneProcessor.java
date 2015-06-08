import java.io.IOException;

import job1.CustomImageReaderWriter;
import job1.VideoSceneMapper;
import job1.VideoSceneReducer;
import job2.CreatePobTableMapper;
import job2.CreatePobTableReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class VideoSceneProcessor extends Configured implements Tool {


	
	   
	   //Parsing MapReduce Job 1
	    public boolean job1CreateSceneInfo(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();
	 

	        Job job = Job.getInstance(conf, "video scene processing");
	        job.setJarByClass(VideoSceneProcessor.class);
	        
	        // Input -> Mapper -> Map
	        FileInputFormat.addInputPath(job, new Path(inputPath));	        
	        job.setInputFormatClass(SequenceFileInputFormat.class);	        
	        job.setMapperClass(VideoSceneMapper.class);	     

	        // Map -> Reducer -> Output
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));	     

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CustomImageReaderWriter.class);   
			job.setOutputFormatClass(TextOutputFormat.class);
	        
	        
	        job.setReducerClass(VideoSceneReducer.class);

	        return job.waitForCompletion(true);
	    }
	    
		private boolean job2CreateProbTables(String inputPath,
				String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		 

        Job job = Job.getInstance(conf, "scene Prob Table creation");
        
        job.setJarByClass(VideoSceneProcessor.class);
        
        // Input -> Mapper -> Map
        FileInputFormat.addInputPath(job, new Path(inputPath));	    
        
     
              
        job.setMapperClass(CreatePobTableMapper.class);	     

        // Map -> Reducer -> Output
        FileOutputFormat.setOutputPath(job, new Path(outputPath));	     

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);           
        
        job.setReducerClass(CreatePobTableReducer.class);        
     
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
		}

	   
	    
	    public static void main(String[] args) throws Exception {
	        System.exit(ToolRunner.run(new Configuration(), new VideoSceneProcessor(), args));
	    }
	    
	    @Override
	    public int run(String[] args) throws Exception {
	        //Run the first MapReduce Job, parsing links from the large dump of wikipedia pages
	    	boolean isCompleted = false;
	    	
	     /*   boolean isCompleted = job1CreateSceneInfo("CNNSequenceFile", "outputCNN/scene");
	        if (!isCompleted) return 1;
*/
	        String  lastResultPath = "outputCNN/scene/part-r-00000";


	        isCompleted = job2CreateProbTables(lastResultPath, "outputCNN/ProbTable");

	        if (!isCompleted) return 1;
	        
	        
	        return 0;
	    }
	    
	



}