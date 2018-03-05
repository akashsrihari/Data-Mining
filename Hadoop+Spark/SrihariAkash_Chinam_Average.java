import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SrihariAkash_Chinam_Average {
         
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
        
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException { 
 StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
        String s = itr.nextToken("\n");
        String[] s1 = s.split(",");
        if(s1[3].length()>0){
                s1[3] = s1[3].replaceAll("['-]","");
                s1[3] = s1[3].replaceAll("[\\^\\[\\],.!?\"/:;@_(){}$&]"," ");
                s1[3] = s1[3].toLowerCase();
                s1[3] = s1[3].trim();
		s1[3] = s1[3].replaceAll(" +"," ");
                word.set(s1[3]);
		if(s1[3].length()<=0)
			continue;
        }
        else
                continue;
        if(s1[18].length()>0)
	if(Character.isDigit(s1[18].charAt(0))){
		one.set(Integer.parseInt(s1[18]));
		context.write(word, one);}
	else
	continue;
	else continue;
      }
    }
  }

  public static class IntSumReducer
  	extends Reducer<Text,IntWritable,Text,DoubleWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Integer sum = 0;
	Integer counter = 0;
      for (IntWritable val : values) {
        sum += val.get();
	counter+=1;
      }

      result.set(sum);
	DoubleWritable dub = new DoubleWritable((double)sum/counter);
	String op = new String(key.toString());
	op = op.concat("\t");
	op = op.concat(counter.toString());
	key = new Text(op);	
      context.write(key, dub);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "average");
    job.setJarByClass(SrihariAkash_Chinam_Average.class);
    job.setMapperClass(TokenizerMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);	
  }
}

