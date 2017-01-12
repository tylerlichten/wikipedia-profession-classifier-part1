package code.inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import utilHadoop05.StringIntegerList;
import utilHadoop05.StringIntegerList.StringInteger;

/**
 * This class is used for Section 2.3.2 of Assignment 1. It takes the Article
 * Lemma Index as input, and runs a map-reduce job to build from it an inverted
 * index. Output form is: lemma List<article_Title, count> where count is
 * the number of times that the given lemma appears in the article. Each lemma's
 * sub-index gets its own line in the output.
 */
public class InvertedIndexMapred {                  
	//NOTE: LongWritable changed from Text, since we are using the default input format for the Map.
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, StringInteger> {

        //Text object for writing each lemma to output
        private final Text nextLemma = new Text();

        //map function called for each article's sub-index (one per line) in input index
        //NOTE: LongWritable lineID changed from Text ArticleID
        @Override
        public void map(LongWritable lineID, Text indices, Context context) throws IOException,
                InterruptedException {

        	StringTokenizer indexTokenizer = new StringTokenizer(indices.toString());
            //gets article title, which is first token of inputted line
            String articleTitle = indexTokenizer.nextToken();
            //If article contains no words there is no need to go further.
            if(!indexTokenizer.hasMoreTokens()){
            	return;
            }
            //gets words and count.
            String wordsCounts = indexTokenizer.nextToken();

            /* replaces formatting characters from index with spaces */
            String indicesString = wordsCounts.replaceAll("<|>|(),", " ");
            StringTokenizer itr = new StringTokenizer(indicesString);

            //emits output of form <Lemma, StringInteger<article_Title, count>> for each lemma
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                int count = Integer.valueOf(itr.nextToken());
                StringInteger articleIdAndCount = new StringInteger(articleTitle.replaceAll("\\|", " "), count);
                nextLemma.set(word);
                context.write(nextLemma, articleIdAndCount);
            }
        }
    }

    public static class InvertedIndexReducer extends
            Reducer<Text, StringInteger, Text, StringIntegerList> {

        /*for each lemma key, aggregates all <article_Title, count> StringInteger objects into a
          StringIntegerList and writes it as output value, mapped to lemma key*/
        @Override
        public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
                throws IOException, InterruptedException {
        	
            //list of StringInteger objects, to be passed to StringIntegerList constructor
            List<StringInteger> listOfSL = new ArrayList<StringInteger>();
            
            for (StringIntegerList.StringInteger sl : articlesAndFreqs) {
                /*NOTE: copy is used here to avoid bizarre error where data pointed to by sl actually changes
                  at next iteration*/
                StringIntegerList.StringInteger copy = new StringIntegerList.StringInteger(sl.getString(), sl.getValue()); 
                listOfSL.add(copy);
            }
            
            StringIntegerList sli = new StringIntegerList(listOfSL);
  
            context.write(lemma, sli);
        }
    }
    
    //configures and runs map-reduce job
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2){
			System.err.println("Usage: Inverted-Index-Mapred <in> <out>");
			System.exit(2);
		}
		
        Job job = Job.getInstance(conf, "Inverted Index Map-Reduce");
        job.setJarByClass(InvertedIndexMapred.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringIntegerList.StringInteger.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringIntegerList.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}