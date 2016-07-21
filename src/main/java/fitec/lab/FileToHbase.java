

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FileToHbase {

	public static class ImportMapper extends Mapper<LongWritable, Text,ImmutableBytesWritable , Mutation>{
		@Override
		protected void map(LongWritable key, Text line,
				Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation>.Context context)
						throws IOException, InterruptedException {
			
			
			String columns = context.getConfiguration().get("conf.columns");
			String[]  cols = columns.split("_");
			String lineString = line.toString();		
			String[] words= lineString.split(" ");
			
			for(int i=0;i<cols.length;i++){
				String tmp[] = cols[i].split(":");
				String family = tmp[0];
				String qualifier = tmp[1];
				byte[] rowkey = DigestUtils.md5(lineString);
				Put put = new Put(rowkey);
								
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(words[i]));
				context.write(new ImmutableBytesWritable(rowkey), put);
			}
			
			
			
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs =new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 3){
			System.err.println("usage : <name_table (must exist)> <name_file_input> <namecolumn1_namecolumn2_..._namecolumnN>");
			System.exit(1);
		}
		
		String table = otherArgs[0];
		String input = otherArgs[1];
		String columns = otherArgs[2];
		conf.set("conf.columns", columns);
		Job job = Job.getInstance(conf, "Import from file " + input + " into table " + table);
		job.setJarByClass(FileToHbase.class);
	
		job.setMapperClass(ImportMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(input));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
