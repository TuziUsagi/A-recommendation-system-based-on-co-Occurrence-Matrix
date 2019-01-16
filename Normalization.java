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
import java.util.HashMap;
import java.util.Map;


public class Normalization
{
    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //input value = movieA_id:movieB_id \t co-occurrence
            if(value.toString() =="")
            {
                return;
            }
            String[] pair_relation = value.toString().trim().split("\t");
            if (pair_relation.length != 2)
            {
                return;
            }
            String movie_col = pair_relation[0].split(":")[1];
            String movie_row = pair_relation[0].split(":")[0];
            String relation = movie_row + "=" + pair_relation[1];

            context.write(new Text(movie_col), new Text(relation));
            //output = movie_col \t movie_row=relation
        }
    }
    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            //create hash map to store the normalized co-occurrence relation
            Map<String, Integer> hmap = new HashMap<String, Integer>();
            int total = 0;
            for (Text value:values)
            {
                String movie_row = value.toString().split("=")[0];
                int relation = Integer.parseInt(value.toString().split("=")[1]);
                hmap.put(movie_row, relation);
                total += relation;
            }
            // iterate the map and output the normalized relation
            for (String mapKey:hmap.keySet())
            {
                String movie_row = mapKey;
                double norm_relation = hmap.get(mapKey)/total;
                String output = movie_row + "=" + norm_relation;

                context.write(key, new Text(output));
                //output = movie_col \t movie_row=normalized_relation
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalization.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
