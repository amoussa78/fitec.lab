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

	public class LastFmListening {

		public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

			private final int USERID_INDEX = 1;
			private final int LOCALLISTENING_INDEX = 2;
			private final int RADIOLISTENING_INDEX = 3;
			private final int SKIP_INDEX = 4;

			@Override
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				
				String[] attributesTab = value.toString().split(" ");
				
				Text listening = new Text();
				Text trackId = new Text();	
				String listeningString = Integer.toString(Integer.parseInt(attributesTab[RADIOLISTENING_INDEX]) + Integer.parseInt(attributesTab[LOCALLISTENING_INDEX]));
				listening.set(listeningString + "--" + attributesTab[SKIP_INDEX]);
				trackId.set(attributesTab[USERID_INDEX]);
				
				context.write(trackId, listening);
			}
		}
		
		public static class PriceSumReducer extends Reducer<Text, Text, Text, Text> {
			
			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {

				Text result = new Text();
				int listening = 0;
				int skip = 0;
				
				for (Text value : values) {
					String attributes[] = value.toString().split("--");
					listening += Integer.parseInt(attributes[0]);
					skip += Integer.parseInt(attributes[1]);
				}
				result.set(listening + " " + skip);
				context.write(key, result);
			}
		}

		public static void main(String[] args) throws Exception {
			
			Job job = new org.apache.hadoop.mapreduce.Job();
			job.setJobName("LastFM Listener by trackId");
			
			job.setJarByClass(LastFmListening.class);
			
			job.setMapperClass(TokenizerMapper.class);
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
