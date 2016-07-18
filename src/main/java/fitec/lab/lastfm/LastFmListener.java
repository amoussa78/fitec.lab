package fitec.lab.lastfm;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LastFmListener {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private final int USERID_INDEX = 0;
		private final int TRACKID_INDEX = 1;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] attributesTab = value.toString().split(" ");
			
			Text userId = new Text();
			Text trackId = new Text();			
			userId.set(attributesTab[USERID_INDEX].toString());
			trackId.set(attributesTab[TRACKID_INDEX].toString());
			
			context.write(trackId, userId);
		}
	}
	
	public static class PriceSumReducer extends Reducer<Text, Text, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<Text> usersId, Context context)
				throws IOException, InterruptedException {

			IntWritable result = new IntWritable();
			Set<Text> usersSet = new HashSet();
			
			for (Text userId : usersId) {
				usersSet.add(userId);
			}
			result.set(usersSet.size());
			System.out.println("fin reducer");
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		
		Job job = new org.apache.hadoop.mapreduce.Job();
		job.setJarByClass(LastFmListener.class);
		job.setJobName("LastFM Listener by trackId");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		job.setNumReduceTasks(1);
		
		job.setJarByClass(LastFmListener.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(PriceSumReducer.class);
		job.setReducerClass(PriceSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
