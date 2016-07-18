package fitec.lab.booble;

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

public class BoobleByMonth {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private final int DATE_INDEX = 0;
		private final int WORDS_INDEX = 3;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] attributesTab = value.toString().split(" ");
			
			Text reduceKey = new Text();
			Text words = new Text();

			String date = attributesTab[DATE_INDEX];
			String[] timeSplitted = date.split("_");

			String month = timeSplitted[1];

			reduceKey.set(getMonth(Integer.parseInt(month)));
			
			words.set(attributesTab[WORDS_INDEX]);
			context.write(reduceKey, words);
		}

		public Text getMonth(int month) {
			Text result = new Text();
			switch (month) {
			case 1:
				result.set("janvier");
				break;
			case 2:
				result.set("fevrier");
				break;
			case 3:
				result.set("mars");
				break;
			case 4:
				result.set("avril");
				break;
			case 5:
				result.set("mai");
				break;
			case 6:
				result.set("juin");
				break;
			case 7:
				result.set("juillet");
				break;
			case 8:
				result.set("aout");
				break;
			case 9:
				result.set("septembre");
				break;
			case 10:
				result.set("novembre");
				break;
			case 11:
				result.set("octobre");
				break;
			case 12:
				result.set("decembre");
				break;

			}
			return result;
		}
	}

	public static class PriceSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> groupedWords, Context context)
				throws IOException, InterruptedException {
			Text result = new Text();
			int requestCount = 0;
			Map<String, Integer> firstWordAndRequestCount = new HashMap<String, Integer>();
			for (Text words : groupedWords) {
				++requestCount;
				String wordsString = words.toString().replace("+", "--");
				System.out.println(wordsString.toString());
				String[] wordTab = wordsString.split("--");
				for (String word : wordTab) {

					if (firstWordAndRequestCount.containsKey(word)) {
						Integer integer = firstWordAndRequestCount.get(word) + 1;
						firstWordAndRequestCount.put(word, integer);
					} else {
						firstWordAndRequestCount.put(word, new Integer(1));
					}
				}
			}

			ValueComparator valueComparator = new ValueComparator(firstWordAndRequestCount);
			TreeMap<String, Integer> sortedProductsSale = new TreeMap<String, Integer>(valueComparator);
			sortedProductsSale.putAll(firstWordAndRequestCount);
			result.set(sortedProductsSale.firstKey() + "__" + requestCount);
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
		job.setJarByClass(BoobleByMonth.class);
		job.setJobName("Booble mot le plus recherché et somme de requete par mois");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(BoobleByMonth.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(PriceSumReducer.class);

		job.setNumReduceTasks(12);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
