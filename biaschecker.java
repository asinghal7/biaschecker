package sparkass3;


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

//Set appropriate package name

//Set appropriate package name

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * This class uses Dataset APIs of spark to count number of articles per month
 * The year-month is obtained as a dataset of Row
 * */

public class Assignment {

	public static void main(String[] args) throws FileNotFoundException {
		
		//Input dir - should contain all input json files
		String inputPath="/home/zedd/Desktop/CS 631/spark/newsdata"; //Use absolute paths 
		String entinputPath="/home/zedd/Desktop/CS 631/spark/entities.txt"; //Use absolute paths 
		String posinputPath="/home/zedd/Desktop/CS 631/spark/positive-words.txt";
		String neginputPath="/home/zedd/Desktop/CS 631/spark/negative-words.txt";
		String outputPath="/home/zedd/Desktop/CS 631/spark/sparkassgnmt";

		//Ouput dir - this directory will be created by spark. Delete this directory between each run
		
		StructType structType = new StructType();
	    structType = structType.add("source-name", DataTypes.StringType, false); // false => not nullable
	    structType = structType.add("year-month", DataTypes.StringType, false); // false => not nullable
	    structType = structType.add("entity", DataTypes.StringType, false);
	    structType = structType.add("sentiment", DataTypes.IntegerType, false); // false => not nullable
	    ExpressionEncoder<Row> fullRowEncoder = RowEncoder.apply(structType);
		
		SparkSession sparkSession = SparkSession.builder()
				.appName("Month wise news articles")		//Name of application
				.master("local")								//Run the application on local node
				.config("spark.sql.shuffle.partitions","2")		//Number of partitions
				.getOrCreate();
		
		Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);
		
		// Dataset<String> entityDataset=sparkSession.read().option("multiLine", true).textFile(entinputPath);   
		// Dataset<String> posDataset=sparkSession.read().option("multiLine", true).textFile(posinputPath);
		// Dataset<String> negDataset=sparkSession.read().option("multiLine", true).textFile(neginputPath);
		
	    Scanner file = new Scanner(new File(entinputPath));

        HashSet<String> entity = new HashSet<String>();
        // For each word in the input
        while (file.hasNext()) {
            // Convert the word to lower case, trim it and insert into the set
            // In this step, you will probably want to remove punctuation marks
            entity.add(file.next());
        }
        
        
        Scanner filep = new Scanner(new File(posinputPath));

        HashSet<String> posw = new HashSet<String>();
        // For each word in the input
        while (filep.hasNext()) {
            // Convert the word to lower case, trim it and insert into the set
            // In this step, you will probably want to remove punctuation marks
            posw.add(filep.next());
        }
        
        
        Scanner filen = new Scanner(new File(neginputPath));

        HashSet<String> negw = new HashSet<String>();
        // For each word in the input
        while (filen.hasNext()) {
            // Convert the word to lower case, trim it and insert into the set
            // In this step, you will probably want to remove punctuation marks
            negw.add(filen.next());
        }
        
        
        /*System.out.println("Iterating over list:"); 
        Iterator<String> a = entity.iterator(); 
        while (a.hasNext()) 
            System.out.println(a.next()); 
        System.out.println("Iterating over list:"); 
        Iterator<String> b = posw.iterator(); 
        while (b.hasNext()) 
            System.out.println(b.next());
        System.out.println("Iterating over list:"); 
        Iterator<String> c = negw.iterator(); 
        while (c.hasNext()) 
            System.out.println(c.next());*/
        
        
		//read it into hashset<string>  enti 
		
		
		Dataset<Row> finalDataset=inputDataset.flatMap(new FlatMapFunction<Row,Row>(){
			public Iterator<Row> call(Row row) throws Exception {
				// The first 7 characters of date_published gives the year-month 
				ArrayList<Row> al = new ArrayList<Row>();
				
				String yearMonthPublished=((String)row.getAs("date_published")).substring(0, 7);
				String sourcename=(String)row.getAs("source_name");
				String article=((String)row.getAs("article_body")); //
				article = article.toLowerCase().replaceAll("[^A-Za-z]", " ");
				article = article.replaceAll("( )+", " ");
			    article = article.trim();
			    //System.out.println(article);
			    String[] word = article.split(" ");
			    
				for (int j=0; j<word.length; j++)
				{	
    			    //System.out.println("word check loop entered");
	                if(entity.contains(word[j])) 
	                {
	                	//System.out.println("match with entity");
	                	if(j>4 && j< (word.length - 5) ) 
	                	{
	                		for(int i=j-5;i<j+6;i++) 
	                		{
	                			boolean noword = true;
	                			if(posw.contains(word[i]))
	        	                {
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], 1));
	                				//System.out.println("match with pos"+i);
	                				noword = false;
	  
	        	                }	
	                			if(negw.contains(word[i]))
	                			{
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], -1));
	                				noword = false;
	                				
	                			}
	                			if(noword)
	                			{
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], 0));
	                			}
	                		}
	                	}	
	                	else if(j >= (word.length - 5) ) 
	                	{
	                		for(int i=j-5;i< word.length;i++) 
	                		{
	                			boolean noword = true;
	                			if (posw.contains(word[i])) 
	                			{
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], 1));
	                				noword = false;
	                				
	                			}	
	                			
	                			if(negw.contains(word[i]))
	                			{
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], -1));
	                				noword = false;
	                				
	                			}
	                			if(noword)
	                			{
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], 0));
	                			}
	                		}
	                	}
	                	else 
	                	{
	                		for(int i=0;i<j+6;i++) 
	                		{
	                			boolean noword = true;
	                			if(posw.contains(word[i]))
	                			{
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], 1));
	                				noword = false;
	                				
	                			}	
	                		
	                			if(negw.contains(word[i]))
	                			{
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], -1));
	                				noword = false;
	                				
	                			}
	                		
	                			if(noword)
	                			{
	                				al.add(RowFactory.create(yearMonthPublished, sourcename, word[j], 0));
	                			}
	                		}
	                	}
	                		
	                }
				}    
                // RowFactory.create() takes 1 or more parameters, and creates a row out of them.
				//Row returnRow=RowFactory.create(yearMonthPublished);
				
				return al.iterator();	  
			}
			
		}, fullRowEncoder);
		
		
		//Then get count group by (source_name, year_month, entity, sentiment)  -- raw sentiment count
		//  output:   (source_name, year_month, entity, sentiment, count)
		//Read multi-line JSON from input files to dataset
		
		Dataset<Row> masterset = finalDataset.where("sentiment=1 or sentiment=-1").groupBy("source-name","year-month", "entity").agg(functions.count("sentiment").as("total_support"),functions.sum("sentiment").as("overall_sentiment")).where("total_support>4");
		Dataset<Row> finalanswer = masterset.drop("total_support");	 
		finalanswer = finalanswer.sort("overall_sentiment");
		
		/*Dataset<Row> supportfiltered=sentimentsum.map(new MapFunction<Row,Row>(){
			public Row call(Row row) throws Exception {
				// The first 7 characters of date_published gives the year-month 
				Integer totalsupport = ((Integer)row.getAs("count"));
				if(totalsupport>4)
				{
					
				}
                // RowFactory.create() takes 1 or more parameters, and creates a row out of them.
				Row returnRow=RowFactory.create(yearMonthPublished);

				return returnRow;	  
			}
			
		}, dateRowEncoder);
		*/
		
		
		
		
		//Dataset<Row> supportfiltered = sentimentsum.where("count>4"); 

		//rawsentiment.show();
		//masterset.show();
		//supportfiltered.show();
		//finalanswer.show();
		// Apply the map function to extract the year-month
		
		finalanswer.toJavaRDD().saveAsTextFile(outputPath);	
		//Dataset<Row> filteredfinal = rawsentiment.
		
		// Group by the desired column(s) and take count. groupBy() takes 1 or more parameters
		//entityDataset.show();
		//posDataset.show();
		//negDataset.show();
        file.close();
        filep.close();
        filen.close();
	}
	
}
