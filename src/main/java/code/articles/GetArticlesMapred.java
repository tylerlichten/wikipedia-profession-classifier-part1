package code.articles;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

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

/**
 * This class is used for Section A of the Assignment. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 * @author hadoop05
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();
		private static final String PEOPLE_FILE = "people.txt";
		// this HashMap will hold the peoples' names
		private HashMap<String, Boolean> peopleMap; 

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			InputStreamReader isr ;
			// this HashMap will hold the peoples' names
			peopleMap = new HashMap<String, Boolean>(); 
			InputStream is = this.getClass().getClassLoader().getResourceAsStream(PEOPLE_FILE);
			
			if (is == null){
				System.out.println("Error while getting resource from "+ PEOPLE_FILE);
				return;
			}
			// read from within the JAR
			isr = new InputStreamReader(is);
			BufferedReader input = new BufferedReader(isr);
			String line;
			try {
				while(input.ready()){
					line = input.readLine();
					if(!line.isEmpty())
						peopleMap.put(line, true);
				}
				input.close();
			} catch(IOException ioe){
				System.out.println("Error while reading from file " + PEOPLE_FILE);
				ioe.printStackTrace();
				return ;
			}
			System.out.println("Okay, parsed "+ PEOPLE_FILE+ " !!");
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			// Mapper to get the articles that contain a name of people.txt file
			
			Text outputKey, outputValue;
			String titleStr;
			outputKey = new Text();
			//output key is set to empty string to not interfere with xml formatting
			outputKey.set("");
			outputValue = new Text();
			
			titleStr = inputPage.getTitle();
			if(peopleMap.containsKey(titleStr)){
				outputValue.set(inputPage.getRawXML());
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2){
			System.err.println("Usage: get-articles-mapred <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "get articles");
		job.setJarByClass(GetArticlesMapred.class);
		job.setMapperClass(GetArticlesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(utilHadoop05.WikipediaPageInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
