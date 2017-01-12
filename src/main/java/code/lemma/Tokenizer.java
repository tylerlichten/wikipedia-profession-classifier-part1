package code.lemma;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import org.apache.commons.io.FileUtils;

public class Tokenizer {

    protected StanfordCoreNLP pipeline;
    private Pattern pat;
    private Set<String> stopwords = new HashSet<String>();
    private final String STOPWORDS_FILE = "stopwords.txt";

    private final boolean TEST_PRINT = false;

    public Tokenizer() {
        //setup Standford NLP for lemmatization
        Properties properties = new Properties();
        properties.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        this.pipeline = new StanfordCoreNLP(properties);
        populateStopwords();

    }
    
    // returns null if sentence is null or empty!
    public List<String> tokenize(String sentence) {
        //create patterns for static field pat
        createPatterns();
        //final string to lemmatize
        String lemmatizeMe = "";
        //list of strings to lemmatize
        if (sentence == null || sentence.trim().isEmpty())
        	return new LinkedList<String>();
        
        List<String> lemmatizeMeList = new LinkedList<String>();
        
        if(TEST_PRINT){
        	System.out.println("-----------------------");
        	System.out.println("BEFORE PATTERN MATCH: " + sentence);
        }
        
        // delete everything that is not alphanumeric, or punctuation, or empty string
        sentence.replaceAll("[^\\w\\p{Punct}\\s]", "");
        // delete the upside down exclamation mark, it is not in the punctuation list!
        sentence.replaceAll("¡", "");
        //match patterns and replace
        Matcher match = pat.matcher(sentence);
        sentence = match.replaceAll(" ").trim();
        
        if(TEST_PRINT) System.out.println("AFTER PATTERN MATCH: " + sentence);

        // be careful! The first character looks like a space but is a different ASCII character... Same for dashes, we have 3 different dashes.
        StringTokenizer words = new StringTokenizer(sentence, "  –-—,.|");
        
        while (words.hasMoreTokens()) {
            String untokenized = words.nextToken();
            String tokenized = tokenizeWord(untokenized);
            if (!tokenized.equals("") && !stopwords.contains(tokenized.toLowerCase())) {
                if(TEST_PRINT) System.out.println("FINAL WORD: " + tokenized);
                //add to list to lemmatize
                lemmatizeMeList.add(tokenized);
            }
        }
        
        //builds list into final string
        lemmatizeMe = String.join(" ", lemmatizeMeList);
        
        if(TEST_PRINT) System.out.println("BEFORE LEMMATIZING: " + lemmatizeMe);
        
        //get list of lemmas
        List<String> listOfLemmas = StanfordLemmatize(lemmatizeMe);
        
        if(TEST_PRINT) System.out.println(listOfLemmas);

        
        List<String> lemmasFinal = new LinkedList<String>();
        for (String lemma : listOfLemmas) {
            String newlemma = lemma.toLowerCase();
          /*gets rid of any tokens that are single characters or all numbers and/or apostrophes
           * or contain any punctuation except apostrophes*/
            if (!stopwords.contains(newlemma) && !newlemma.matches(".{1}|[0-9']+") 
            								  && !newlemma.matches("^.*[�\\p{Punct}&&[^']]+.*$")) {
            	lemmasFinal.add(newlemma);
            }

        }
        
        if(TEST_PRINT) System.out.println("-----------------------");

        return lemmasFinal;
    }

    /*tokenizes one untokenized element into a list of tokenized elements
      Note that only letters are allowed*/
    private static String tokenizeWord(String word) {
        word = tokenizeLead(word);
        word = tokenizeTail(word);
        //returns "" if a single character or all numbers or all numbers with an optional final 's'
        if (word.matches(".{1}|\\d*'?(rd|th|s|nd|st)?")) {
            return "";
        }
        return word;
    }

    //removes non-word characters from beginning of word
    public static String tokenizeLead(String word) {
        return word.replaceFirst("^'*", "");
    }
    //removes non-word characters from end of word, as well as select contractions
    public static String tokenizeTail(String word) {
        return word.replaceFirst("'*$", "");
    }

    public void createPatterns() {
        //list of patterns
        List<String> patterns = new ArrayList<>();
        // references
        patterns.add("\\<(.*?)\\>");
        //characters to not keep
        //patterns.add("�");
        patterns.add("[�\\p{Punct}&&[^']]");
        // italics and bold
        patterns.add("''+");
        // we do not need most of these patterns, since article.getContent() cleans up everything!
        // url
        //patterns.add("\\[http.*\\]");
        //website without http
        //patterns.add("[\\w\\p{Punct}]+\\.(com|org|edu|net)");
        // infobox if needed
        //patterns.add("\\{\\{Infobox.*\\}\\}");
        // file
        //patterns.add("\\[\\[File:.*\\]\\]");
        // image
        //patterns.add("\\[\\[Image:.*\\]\\]");
        // media
        //patterns.add("\\[\\[media:.*\\]\\]");
        //External Links
        //patterns.add("[Ee]xternal [Ll]inks");
        // dates if needed
        //patterns.add("\\|(access)?date=.*(\\||\\})");
        //image names
        //patterns.add("[\\w-]+\\.(jpg|png|tiff|gif|bmp)");
        
        // citation <ref> and cite webs in between in HTML
        //patterns.add("&lt;ref&gt;.*&lt;\\/ref&gt;");
        // <rsomething else> in HTML
        //patterns.add("&lt;.*&gt;");
        // & in HTML
        //patterns.add("&amp;");
        // " in HTML
        //patterns.add("&quot;");
        //build one string of all patterns
        StringBuilder builder = new StringBuilder();
        for (String each : patterns) {
            // group and separate each pattern with OR
            builder.append("(" + each + ")|");
        }
        // remove extra OR at end due to append
        builder.deleteCharAt(builder.length()-1);
        //line breaks and compile pattern pat
        pat = Pattern.compile("(" + builder.toString() + ")+", Pattern.DOTALL);
    }

    //Stanford NLP for lemmatization
    public List<String> StanfordLemmatize(String lemmatizeMe) {
        //build list of lemmatized words
        List<String> listOfLemmas = new LinkedList<String>();
        Annotation doc = new Annotation(lemmatizeMe);
        this.pipeline.annotate(doc);
        List<CoreMap> line = doc.get(SentencesAnnotation.class);
        for (CoreMap each : line) {
            for (CoreLabel itr : each.get(TokensAnnotation.class)) {
                listOfLemmas.add(itr.getString(LemmaAnnotation.class));
            }
        }

        listOfLemmas.removeAll(stopwords);
        return listOfLemmas;
    }

    public void populateStopwords () {
        InputStreamReader isr ;
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(STOPWORDS_FILE);

        if (is == null){
            System.out.println("Error while getting resource from "+ STOPWORDS_FILE);
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
                    stopwords.add(line);
            }
            input.close();
        } catch(IOException ioe){
            System.out.println("Error while reading from file " + STOPWORDS_FILE);
            ioe.printStackTrace();
            return ;
        }
    }

	//Main method for testing. Update with your own path
	public static void main(String[] args) throws IOException {
		File f = new File("PATH TO FILE");
		String str = FileUtils.readFileToString(f, "UTF-8");
		Tokenizer tokenizer = new Tokenizer();
		StringTokenizer strTok = new StringTokenizer(str, "\n");
		while(strTok.hasMoreTokens()){
			String line = strTok.nextToken();
			tokenizer.tokenize(line);
		}
		
	}
}