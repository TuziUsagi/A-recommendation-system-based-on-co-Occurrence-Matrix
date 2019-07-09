package recommenderSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiplication
{
    public static class coOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //input = movie_col \t movie_row=normalized_relation
            if (value.toString() == "")
            {
                return;
            }
            String[] relation = value.toString().trim().split("\t");
            if (relation.length !=2)
            {
                return;
            }
            String movie_col = relation[0];
            String movie_row_relation = relation[1];

            //output key: movie_id in column, output value: movie_id in row=relation
            context.write(new Text(movie_col), new Text(movie_row_relation));

        }
    }

    public static class ratingMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // input file = user,movie,rating
            if (value.toString() == "")
            {
                return;
            }
            String[] ratings = value.toString().trim().split(",");
            if (ratings.length !=3)
            {
                return;
            }
            String movie_id = ratings[1];
            String user_rating = ratings[0]+ ":" + ratings[2];

            //output key: movie_id, output value: user_id:rating
            context.write(new Text(movie_id), new Text(user_rating));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            Map<String, Double> relation_hash = new HashMap<String, Double>();
            Map<String, Double> rating_hash = new HashMap<String, Double>();
            for (Text value: values)
            {
                if(value.toString().contains("="))
                {
                    String movie_row = value.toString().split("=")[0];
                    double relation = Double.parseDouble(value.toString().split("=")[1]);
                    relation_hash.put(movie_row, relation);
                }
                if(value.toString().contains(":"))
                {
                    String user_id = value.toString().split(":")[0];
                    double rating = Double.parseDouble(value.toString().split(":")[1]);
                    rating_hash.put(user_id,rating);
                }
            }
            for (String relation_key:relation_hash.keySet())
            {
                String movie_row = relation_key;
                double norm_relation = relation_hash.get(relation_key);

                for (String rating_key : rating_hash.keySet()) {
                    String user_id = rating_key;
                    double rating = rating_hash.get(rating_key);
                    double weight = norm_relation * rating;
                    String user_movie = user_id + "," + movie_row;
                    //output: user_id,movie_id \t weight
                    context.write(new Text(user_movie), new DoubleWritable(weight));
                }
            }
            
        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MatrixMultiplication.class);

        ChainMapper.addMapper(job, coOccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, ratingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(coOccurrenceMapper.class);
        job.setMapperClass(ratingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, coOccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ratingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
