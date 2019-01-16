package recommenderSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SumMovieWeights
{
    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
    {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //input: user_id,movie_id \t weight
            if (value.toString() == "")
            {
                return;
            }
            if (value.toString().length() !=2)
            {
                return;
            }
            String user_movie = value.toString().trim().split("\t")[0];
            double weight  = Double.parseDouble(value.toString().trim().split("\t")[1]);
            context.write(new Text(user_movie), new DoubleWritable(weight));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            double sumWeight = 0.0;
            for (DoubleWritable value: values)
            {
                sumWeight += value.get();
            }
            context.write(key, new DoubleWritable(sumWeight));
        }
    }

    public static void main(String[] args) throws Exception
    {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setJarByClass(SumMovieWeights.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
