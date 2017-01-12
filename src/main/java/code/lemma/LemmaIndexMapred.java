package code.lemma;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import utilHadoop05.StringIntegerList;

/**
 * This class is for building the Lemma Article Index (part 2.3.1 of assignment) from a Wikipedia file
 * that has been filtered for articles whose titles match one of the names in people.txt. Note that
 * tokenization, cleaning, and lemmatization (part 2.2) are performed here as part of the
 * map function. Also note that only the map function of map-reduce is required. Output is of the
 * form article_Title, List<lemma, count>, where count is the number of times that the given lemma appears
 * in the article. Each article's sub-index gets its own line in the output.
 */
public class LemmaIndexMapred {
	
	private static Tokenizer tokenizer = new Tokenizer();
	
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		//article title to be written to output
		Text articleTitle = new Text();

		//map function called for each WikipediaPage
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
				InterruptedException {

			//replaces whitespace chars in name with "|" so that names are a single token
			articleTitle.set(page.getTitle().replaceAll("\\s", "|"));

			//maps words to counts
			Map<String, Integer> wordToCountMap = new HashMap<String, Integer>();
			List<String> pageContents = tokenizer.tokenize(page.getContent());

			//scans page token by token
			for (String word : pageContents) {
				incrementCount(word, wordToCountMap);
			}
			//converts Map<String, Integer> to StringIntegerList
			StringIntegerList index = new StringIntegerList(wordToCountMap);
			//as one line, writes sub-index article_Title, List<lemma, count> to output
			context.write(articleTitle, index);
		}
	}

	//increments lemma count by one
	public static void incrementCount(String name, Map<String, Integer> wordToCountMap) {
		Integer prevCount = wordToCountMap.get(name);
		if (prevCount != null) {
			wordToCountMap.put(name, prevCount + 1);
		}else{
			wordToCountMap.put(name, 1);
		}
			
	}

	//main method for configuring and running map-reduce job
	public static void main (String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2){
			System.err.println("Usage: lemma-index-mapred <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Lemma Index Map-Reduce");
		job.setJarByClass(LemmaIndexMapred.class);
		job.setMapperClass(LemmaIndexMapper.class);
		job.setInputFormatClass(utilHadoop05.WikipediaPageInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringIntegerList.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
