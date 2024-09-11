import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Task1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
        {
  FileSplit fileSplit = (FileSplit) context.getInputSplit();
  String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
  String[] currentRecord = value.toString().split(";");
  int length = currentRecord.length;
  if (length == 5 && "movie".equals(currentRecord[length - 4]) && !currentRecord[length - 2].equals("\\N")) {
  int currentYear = Integer.parseInt(currentRecord[length - 2]);
  String genre = currentRecord[length - 1];
      if(!genre.equals("\\N")){
          for (int year = 2000; year <= 2020; year += 7) 
            if (currentYear>=year && currentYear <= year+6) {
                List<String> genreList = Arrays.asList(genre.split(","));

                    if (genreList.contains("Comedy") && genreList.contains("Romance")){
                        String genreKey = "[" + year + "-" + (year + 6) + "],Comedy;Romance,";
                        word.set(genreKey);
                        context.write(word, one);
                    }
                    
                    if (genreList.contains("Action") && genreList.contains("Drama")) {
                        String genreKey = "[" + year + "-" + (year + 6) + "],Action;Drama,";
                        word.set(genreKey);
                        context.write(word, one);
                    }
                    
                    if (genreList.contains("Adventure") && genreList.contains("Sci-Fi")) {
                        String genreKey = "[" + year + "-" + (year + 6) + "],Adventure;Sci-Fi,";
                        word.set(genreKey);
                        context.write(word, one);
                    }
                }
        }
        }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Job job = Job.getInstance(conf, "task");
    job.setJarByClass(Task1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    Path input =new Path(args[0]);
    Path output =new Path(args[1]);
    FileInputFormat.addInputPath(job, input);
    if (fs.exists(output))
            fs.delete(output, true);
    FileOutputFormat.setOutputPath(job, output);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
