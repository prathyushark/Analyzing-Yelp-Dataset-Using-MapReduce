import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4 {

	public static class TopTenBusiness extends Mapper<Object, Text, Text, DoubleWritable>
	{
	private Text word = new Text();
	
     public void map(Object key, Text value, Context context ) throws IOException, InterruptedException 
     {
    	 String business=null, star=null;
    	 StringTokenizer tokenizer=new StringTokenizer(value.toString(), "^");
    	 int count=0;
    	 String wordl;
    	 
    	 while(tokenizer.hasMoreTokens())
    	 {
    		 count++;
    		 wordl=tokenizer.nextToken().toString();
    		 if(count==3)
    			 business=wordl;
    		 if(count==4)
    			 star=wordl;
   	  	}
   	  
    	 word.set(business);
    	 double rating=Double.parseDouble(star);
    	 context.write(word, new DoubleWritable(rating));
   	
     } // map
 
}//TopTen
	
	 	public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
	 	{
	 		private DoubleWritable result = new DoubleWritable();
	 		private Text word = new Text();
	 		Map<String,Double> map=new HashMap<String,Double>();
      
	 		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	 			int sum = 0, count=0;
	 			for (DoubleWritable val : values) {
	 				sum += val.get();
	 				count++;
         }//reduce
	 			double avg=(double)sum/count;
	 			map.put(key.toString(), avg);  
		 }//Reduce class
		 
		 @Override
		protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context )throws IOException, InterruptedException {
			
			Map<String,Double> sortedMap=sortByValue(map);
			int count=0;
			for(String str: sortedMap.keySet())
			{
				result.set(sortedMap.get(str));
				word.set(str);
				context.write(word, result);
				count++;
				if(count==10)
					break;
			}
			
		}
		 
		 private static Map<String, Double> sortByValue(Map<String, Double> unsortMap) {

		        List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());

		        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
		            public int compare(Map.Entry<String, Double> o1,Map.Entry<String, Double> o2) {
		                return (o2.getValue()).compareTo(o1.getValue());
		            }
		        });

		        Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		        for (Map.Entry<String, Double> entry : list) {
		            sortedMap.put(entry.getKey(), entry.getValue());
		        }

		        return sortedMap;
		    }
  }
	 
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Top Ten Rated Business");
		    job.setJarByClass(Q4.class);
		    job.setMapperClass(TopTenBusiness.class);
		    job.setReducerClass(Reduce.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

}
