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
import java.lang.Exception;

public class UserRatings
{
    public static class userRatingsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //input user,movie,rating
            if (value.toString()== "")
            {
                return;
            }
            String[] ratingRec = value.toString().trim().split(",");
            int user_id = Integer.parseInt(ratingRec[0]);
            String movie_rating = ratingRec[1] + ":" + ratingRec[2];
            context.write(new IntWritable(user_id), new Text(movie_rating));
            // output: user_id \t movie_id:rating

        }
    }

    public static class userRatingsReducer extends Reducer<IntWritable, Text, IntWritable, Text>
    {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            //key = user_id
            //outputValue = list of movie_id:rating
            StringBuilder ratings = new StringBuilder();
            for (Text value:values)
            {
                ratings.append("," + value);
            }
            ratings.substring(1);
            context.write(key, new Text(ratings.toString()));
            //output = user_id \t movie1:2.3,movie2:4.9....
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(userRatingsMapper.class);
        job.setReducerClass(userRatingsReducer.class);

        job.setJarByClass(UserRatings.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
