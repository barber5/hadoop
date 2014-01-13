package co.brbr5.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendCounter extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new FriendCounter(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        Job job = new Job(getConf(), "FriendCounter");
        job.setJarByClass(FriendCounter.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class); // breaks into lines
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("ahh fuck");
        job.waitForCompletion(true);

        return 0;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable > {
        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] splitter = value.toString().split("\t");
            if(splitter.length < 2) {
                return;
            }
            int user = Integer.parseInt(value.toString().split("\t")[0]);
            String[] friendsStr = value.toString().split("\t")[1].split(",");
            for(String friendiStr: friendsStr) {
                IntWritable friendi = new IntWritable(Integer.parseInt(friendiStr));
                String friend = "";
                if(friendi.get() < user) {
                    friend = friendi.get() + ", " + user;
                } else {
                    friend = user + ", " + friendi.get();
                }

                context.write(new Text(friend), new IntWritable(-1));
                for(String friendjStr: friendsStr) {
                    if(friendiStr.equals(friendjStr))
                        continue;
                    IntWritable friendj = new IntWritable(Integer.parseInt(friendjStr));
                    if(friendi.get() < friendj.get()) {
                        friend = friendi.get() + ", "+friendj.get();
                    } else {
                        friend = friendj.get() + ", "+friendi.get();
                    }
                    context.write(new Text(friend), ONE);
                }
            }
        }
    }


    public static class Reduce extends Reducer<Text, IntWritable , Text, IntWritable > {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable tw: values) { // count our mutual friends
                if(tw.get() == -1) {
                    return;
                }
                sum++;
            }

            context.write(key, new IntWritable(sum));
        }
    }
}