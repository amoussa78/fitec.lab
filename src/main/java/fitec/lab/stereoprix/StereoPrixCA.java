package fitec.lab.stereoprix;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StereoPrixCA {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		private final int DATE_INDEX = 0;
		private final int PRICE_INDEX = 2;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] attributesTab = value.toString().split(" ");
			
			LocalDateTime date = LocalDateTime.parse(attributesTab[DATE_INDEX], DateTimeFormatter.ofPattern("dd/MM/yyyy-HH:mm:ss"));
			LocalDateTime aMonthAgo = LocalDateTime.now().minusMonths(12);
			if (date.isAfter(aMonthAgo)) {
				DoubleWritable price = new DoubleWritable();
				price.set(Double.parseDouble(attributesTab[PRICE_INDEX]));
				System.out.println("valeurs = " + attributesTab[PRICE_INDEX]);
				context.write(new Text("1"), price);
			}
		}
	}
	
	public static class PriceSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> prices, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (DoubleWritable price : prices) {
				sum += price.get();
				System.out.println(price.get());
			}
			result.set(sum);
			System.out.println("fin reducer");
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new org.apache.hadoop.mapreduce.Job();
		job.setJarByClass(StereoPrixCA.class);
		job.setJobName("StereoPrix CA 12 mounths");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);
		job.setJarByClass(StereoPrixCA.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(PriceSumReducer.class);
		job.setReducerClass(PriceSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
