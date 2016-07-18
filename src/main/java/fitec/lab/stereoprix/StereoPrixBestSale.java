package fitec.lab.stereoprix;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StereoPrixBestSale {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private final int NAME_INDEX = 3;
		private final int CATEGORY_INDEX = 4;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] attributesTab = value.toString().split(" ");

			Text product = new Text();
			Text category = new Text();
			category.set(attributesTab[CATEGORY_INDEX]);
			product.set(attributesTab[NAME_INDEX]);

			context.write(category, product);
		}
	}

	public static class PriceSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> products, Context context)
				throws IOException, InterruptedException {

			Map<String, Integer> productsSale = new HashMap<String, Integer>();
			for (Text product : products) {
				String productName = product.toString();
				if (productsSale.containsKey(productName)) {
					Integer integer = productsSale.get(productName) + 1;
					productsSale.put(productName, integer);
				} else {
					productsSale.put(productName, new Integer(1));
				}
			}
			ValueComparator valueComparator = new ValueComparator(productsSale);
			TreeMap<String, Integer> sortedProductsSale = new TreeMap<String, Integer>(valueComparator);
			sortedProductsSale.putAll(productsSale);
			result.set(sortedProductsSale.firstKey());
			context.write(key, result);
		}

		class ValueComparator implements Comparator<String> {
			Map<String, Integer> base;

			public ValueComparator(Map<String, Integer> base) {
				this.base = base;
			}

			public int compare(String a, String b) {
				if (base.get(a) >= base.get(b)) {
					return -1;
				} else {
					return 1;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = new org.apache.hadoop.mapreduce.Job();
		job.setJarByClass(StereoPrixBestSale.class);
		job.setJobName("StereoPrix meilleur vente par categorie");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(StereoPrixBestSale.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(PriceSumReducer.class);
		job.setReducerClass(PriceSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
