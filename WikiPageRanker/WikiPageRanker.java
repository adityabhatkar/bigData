/*Aditya Bhatkar 800887086
 * */
import java.io.IOException;
import java.io.StringReader;

//import java.nio.file.DirectoryNotEmptyException;
//import java.nio.file.Files;
//import java.nio.file.NoSuchFileException;
//import org.apache.commons.io.FileUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;



//Driver Class
public class WikiPageRanker  extends Configured implements Tool{

	//set the number of iterations to be done
	private static final int NUMBEROFITERATIONS=3;

	public static void main(String[] args){

		int res=0;
		try {
			res=ToolRunner.run(new WikiPageRanker(), args);
		} catch (Exception e) {

			e.printStackTrace();
		}

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		//job1
		DocParser docParser=new DocParser();

		Job job1= Job.getInstance(getConf(), "docParser");
		job1.setJarByClass(docParser.getClass());

		FileInputFormat.addInputPaths(job1,  args[0]);		
		FileOutputFormat.setOutputPath(job1,  new Path(args[1]+"/Job1"));

		job1.setMapperClass(DocParser.Map.class);
		job1.setReducerClass(DocParser.Reduce.class);

		job1.setOutputKeyClass( Text.class);
		job1.setOutputValueClass( Text.class);		
		job1.waitForCompletion(true);


		//job2 to be done in loop
		int i=0;
		for(i=0; i<NUMBEROFITERATIONS; i++){
			RankCalculator rankCalculator=new RankCalculator();
			Job job2=Job.getInstance(getConf(), "rankCalculator");
			job2.setJarByClass(rankCalculator.getClass());


			job2.setMapperClass(RankCalculator.Map.class);
			job2.setReducerClass(RankCalculator.Reduce.class);

			job2.setOutputKeyClass( Text.class);
			job2.setOutputValueClass( Text.class);	


			//output of job1 becomes input of job2
			if(i==0){
				FileInputFormat.addInputPaths(job2,  args[1]+"/Job1");
			}
			else{				

				FileInputFormat.addInputPaths(job2,  args[1]+"/Job2"+"/step"+(i-1));
			}
			FileOutputFormat.setOutputPath(job2,  new Path(args[1]+"/Job2"+"/step"+i));

			job2.waitForCompletion(true);
		}	




		//job3
		Cleaner cleaner=new Cleaner();

		Job job3= Job.getInstance(getConf(), "cleaner");
		job3.setJarByClass(cleaner.getClass());
		String job3InputPath="";		
		job3InputPath=args[1]+"/Job2"+"/step"+(i-1);
		
		FileInputFormat.addInputPaths(job3,  job3InputPath);
		FileOutputFormat.setOutputPath(job3,  new Path(args[1]+"/Job3"));

		job3.setMapperClass(Cleaner.Map.class);
		job3.setReducerClass(Cleaner.Reduce.class);

		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass( DoubleWritable.class);
		job3.setOutputValueClass( Text.class);
		job3.setSortComparatorClass(LongWritable.DecreasingComparator.class); //set sort criteria 
		job3.setNumReduceTasks(1); //setting one reducer for sorting
		job3.waitForCompletion(true);
		
		//Delete residue files
		//FileUtils.deleteDirectory(new File(job3InputPath));
		FileSystem fileSystem=FileSystem.get(getConf());
		fileSystem.delete(new Path(args[1]+"/Job2/"), true);	
		fileSystem.delete(new Path(args[1]+"/Job1/"), true);
	
		return 1;

	}

}

//Phase 1
//parse the input and identify link
class DocParser extends Configured{

	//setting initial page rank
	private static final double initialPageRank=0.15;

	private static final Logger logger = Logger.getLogger( DocParser.class);
	
	//choosing the delimiter to separate text
	private static final String delimiter="####";

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable offset, Text lineText, Context context){

			String lineXML=lineText.toString();
			//append a root element to enable parsing using dom
			lineXML="<myRoot>"+lineXML+"</myRoot>";

			logger.info("**********************"+lineXML);

			DocumentBuilder builder=null;
			try {

				builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

			} catch (ParserConfigurationException e1) {

				e1.printStackTrace();
			}
			InputSource src = new InputSource();
			src.setCharacterStream(new StringReader(lineXML));

			Document doc=null;
			String title=null;
			String pageText=null;
			try {

				doc = builder.parse(src);
				title= doc.getElementsByTagName("title").item(0).getTextContent();
				pageText= doc.getElementsByTagName("text").item(0).getTextContent();

				logger.info(title+"**********************"+pageText);

			} catch (SAXException e) {				
				e.printStackTrace();
			} catch (IOException e) {				
				e.printStackTrace();
			}

			//parse pageText
			String linkedPage="";		

			
			Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
			Matcher m = p.matcher(pageText);
			while(m.find()) {			
				
				if(m.group()!=null){

					linkedPage=m.group();
					linkedPage=linkedPage.substring(2, linkedPage.length()-2); //[[linkedPage]] hence from index 2 to len-2
					try {
						logger.info(title+"********!!!!!!!!!!!**************"+linkedPage);
						context.write(new Text(title), new Text(linkedPage));

					} catch (IOException e) {						
						e.printStackTrace();
					} catch (InterruptedException e) {						
						e.printStackTrace();
					}

				}
			}
			

		}	

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		public void reduce(Text title, Iterable<Text> linkedPagesIterator,  Context context){


			try {				
				//Collect all outlinks of the page
				Text linkedPages=new Text("");
				for(Text linkedPage: linkedPagesIterator){

					linkedPages=new Text(linkedPages.toString()+delimiter+linkedPage.toString());
					

				}

				context.write(title, new Text(delimiter+initialPageRank+linkedPages.toString()));				

			} catch (IOException e) {				
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

}

//Pahse2
//calculate page rank for each page
class RankCalculator extends Configured{

	private static final Logger logger=Logger.getLogger(RankCalculator.class);
	private static final String delimiter="####";
	//distinguisher used to preserve input 
	private static final String distinguisher="$$$$$";

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable offset, Text lineText, Context context){

			logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!inside RankCalculator Map");
			
			String line=lineText.toString();
			logger.info("!!!!!!!!!!!!!!!!!!!LINE!!!!!!!!!!!!!!!!!!!!!!"+line);
			String[] inputTokens=line.split(delimiter);			

			String title=inputTokens[0].trim();	
			
			int outDegree=inputTokens.length-2;			
			String previousPageRank=inputTokens[1];

			
			for(int i=2; i<inputTokens.length; i++){
			
				Text value=new Text(delimiter+title+delimiter+previousPageRank+delimiter+outDegree);
				try {
					logger.info(inputTokens[i]+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+value);
					context.write(new Text(inputTokens[i]), value);

					//PRINTING original input line output of first map reduce job
					context.write(new Text(title) , new Text(distinguisher+inputTokens[i]));


				} catch (IOException e) {					
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		public void reduce(Text page, Iterable<Text> pageCollection, Context context){

			String value="";			
			Double pageRank=0.0;

			//parsing links
			for(Text parentPageInfo : pageCollection){				

				logger.info("**************************************"+parentPageInfo.toString());

				if(parentPageInfo.toString().contains(distinguisher)){

					value=(value+delimiter+parentPageInfo.toString().split(distinguisher)[0].trim().substring(5,parentPageInfo.toString().split(distinguisher)[0].trim().length()));					

				}
				else{
					 
					String[] infoTokens=parentPageInfo.toString().split(delimiter);				
					
					pageRank=pageRank+(Double.parseDouble(infoTokens[2])/Long.parseLong(infoTokens[3]));
				}

			}			
			//calculating page rank
			pageRank=(((0.85)*pageRank)+(0.15));
			logger.info(page+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Page rank"+pageRank);

			value=delimiter+pageRank+value;

			try {
				logger.info(page+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+value);
				context.write(page, new Text(value));

			} catch (IOException e) {				
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}


	} 
}

//Phase 3
//Sort output 
//Clean unnecessary output data
class Cleaner extends Configured{

	private static final Logger logger = Logger.getLogger(Cleaner.class);
	private static final String delimiter="####";

	public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text>{		

		public void map(LongWritable offset, Text lineText, Context context){

			String[] inputTokens=lineText.toString().split(delimiter);		

			String page=inputTokens[0].trim();						
			Double pageRank=Double.parseDouble(inputTokens[1]);

			logger.info(page+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+pageRank);

			try {
				//write the page rank before page titles to enable sorting 
				context.write(new DoubleWritable(pageRank), new Text(page));

			} catch (IOException e) {				
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	public static class  Reduce extends Reducer<DoubleWritable,Text, DoubleWritable, Text>{

		public void reduce(DoubleWritable pageRank, Iterable<Text> pageCollection, Context context){


			
			logger.info("!!!!!!!!!!!Inside Reduce!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			//print all page titles
			for(Text page:pageCollection){

				try {
					logger.info(page.toString()+"!!!!!!!!!!!Inside Reduce!!!!!!!!!!!!!!!!!!!!!!!!!!!"+pageRank);
					context.write(pageRank, page);

				} catch (IOException e) {
					System.out.println(page.toString()+"!!!!!!!!!!!Inside Reduce!!!!!!!!!!!!!!!!!!!!!!!!!!!"+pageRank);
					e.printStackTrace();
				} catch (InterruptedException e) {					
					e.printStackTrace();
				}

			}

		}
	}


}