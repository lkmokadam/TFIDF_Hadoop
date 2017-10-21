import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/*
 * Main class of the TFIDF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFIDF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }
		
		// Create configuration
		Configuration conf = new Configuration();
		
		// Input and output paths for each job
		Path inputPath = new Path(args[0]);
		Path wcInputPath = inputPath;
		Path wcOutputPath = new Path("output/WordCount");
		Path dsInputPath = wcOutputPath;
		Path dsOutputPath = new Path("output/DocSize");
		Path tfidfInputPath = dsOutputPath;
		Path tfidfOutputPath = new Path("output/TFIDF");
		
		// Get/set the number of documents (to be used in the TFIDF MapReduce job)
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);
		
		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		if (hdfs.exists(dsOutputPath))
			hdfs.delete(dsOutputPath, true);
		if (hdfs.exists(tfidfOutputPath))
			hdfs.delete(tfidfOutputPath, true);
		
		// Create and execute Word Count job
		
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(TFIDF.class);
		job.setMapperClass(WCMapper.class);
		job.setCombinerClass(WCReducer.class);
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, wcInputPath);
		FileOutputFormat.setOutputPath(job, wcOutputPath);
		job.waitForCompletion(true);
			
		// Create and execute Document Size job
		
		Job jobc = Job.getInstance(conf, "data count");
		jobc.setJarByClass(TFIDF.class);
		jobc.setMapperClass(DSMapper.class);
		jobc.setCombinerClass(DSReducer.class);
		jobc.setReducerClass(DSReducer.class);
		jobc.setOutputKeyClass(Text.class);
		jobc.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobc, dsInputPath);
		FileOutputFormat.setOutputPath(jobc, dsOutputPath);
		
		//Create and execute TFIDF job
		
			/************ YOUR CODE HERE ************/
		System.exit(jobc.waitForCompletion(true) ? 0 : 1);
    }
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private static Text data = new Text();
		
		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();		
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				data.set(tokenizer.nextToken()+"@"+fileName);
				context.write(data,new IntWritable(1));		   
		   }
		}
			
		
    }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {

			int sum = 0;
			for( IntWritable value : values){
				sum += value.get();
			}

			context.write(key,new IntWritable(sum));
		}
		
		
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {
		
		private static Text keyData = new Text();
		private static Text valueData = new Text();
		
		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
			int i = 0;
			System.out.println(key.toString());	
			try{
			if( i==0) throw new RuntimeException("-->"+key.toString());
			} catch(RuntimeException e){
				i=0;
			}
			String splittedKey[] = key.toString().split("@");
			if (splittedKey.length != 2) return;
			keyData.set(splittedKey[1]);
			valueData.set(splittedKey[0]+"="+value.toString());

			context.write(keyData,valueData);		   
		}
		
    }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize) 
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
		
			String document = key.toString();
			int total=0;
			ArrayList <String> wordList = new ArrayList<String> ();
			ArrayList <Integer> countList = new ArrayList<Integer> ();
			ArrayList <Integer> wordByCountList = new ArrayList<Integer> ();

			for( Text value : values){
				String[] splittedKey = value.toString().split("=");
				wordList.add(splittedKey[0]);
				countList.add(Integer.parseInt(splittedKey[1]));
				total+=Integer.parseInt(splittedKey[1]);
			} 

			for(Integer count : countList ){
				wordByCountList.add(count/total);
			}

			for( int i = 0 ; i < wordList.size();i++){
				context.write(new Text(wordList.get(i)+"@"+document), new Text(wordByCountList.get(i).toString()));
			}
		}
		
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 * 
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

		/************ YOUR CODE HERE ************/
		
    }

    /*
	 * For each identical key (word), reduces the values (document=wordCount/docSize) into a 
	 * the final TFIDF value (TFIDF). Along the way, calculates the total number of documents and 
	 * the number of documents that contain the word.
	 * 
	 * Input:  ( word , (document=wordCount/docSize) )
	 * Output: ( (document@word) , TFIDF )
	 *
	 * numDocs = total number of documents
	 * numDocsWithWord = number of documents containing word
	 * TFIDF = (wordCount/docSize) * ln(numDocs/numDocsWithWord)
	 *
	 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
	 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs 
	 *       is highly inefficient!
	 */
	//public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
		
		/*private static int numDocs;
		private Map<Text, Text> tfidfMap = new HashMap<>();
		
		// gets the numDocs value and stores it
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numDocs = Integer.parseInt(conf.get("numDocs"));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			/************ YOUR CODE HERE ************/
	 
			//Put the output (key,value) pair into the tfidfMap instead of doing a context.write
			//tfidfMap.put(/*document@word*/, /*TFIDF*/);*/
		//}
		
		// sorts the output (key,value) pairs that are contained in the tfidfMap
		/*protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tfidfMap);
			for (Text key : sortedMap.keySet()) {
                context.write(key, sortedMap.get(key));
            }
        }
		
    }*/
}
