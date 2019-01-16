package recommenderSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class CoOccurrenceMatrix
{
    public static class COMatrixMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //input value = user_id \t movie1:rating1,movie2:rating2,movie3:rating3...
            if(value.toString() == "")
            {
                return;
            }
            String[] userRatings = value.toString().trim().split("\t");
            if (userRatings.length != 2)
            {
                return;
            }
            String[] ratings = userRatings[1].split(",");
            // get every movie combination
            for (int i = 0; i < ratings.length; i++)
            {
                for (int j = 0; j < ratings.length; j++)
                {
                    String moviePair = ratings[i].split(":")[0] + ":" + ratings[j].split(":");
                    context.write(new Text(moviePair), new IntWritable(1));
                    // for each movie combination, output = movieA:movieB \ t 1
                }
            }
        }
    }

    public static class COMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int co_occurrence = 0;
            for (IntWritable value:values)
            {
                co_occurrence += value.get();
            }
            context.write(key, new IntWritable(co_occurrence));
            // output = movieA:movieB \ t c0-occurrence
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(COMatrixMapper.class);
        job.setReducerClass(COMatrixReducer.class);

        job.setJarByClass(CoOccurrenceMatrix.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
