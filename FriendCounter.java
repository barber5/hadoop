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
        job.setOutputKeyClass(IntArrayWritable.class);
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

    public static class Map extends Mapper<LongWritable, Text, IntArrayWritable, IntWritable > {
        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] splitter = value.toString().split("\t");
            if(splitter.length < 100) {
                return;
            }
            int user = Integer.parseInt(value.toString().split("\t")[0]);
            String[] friendsStr = value.toString().split("\t")[1].split(",");
            for(String friendiStr: friendsStr) {
                IntWritable friendi = new IntWritable(Integer.parseInt(friendiStr));
                int[] friend = new int[2];
                if(friendi.get() < user) {
                    friend[0] = friendi.get();
                    friend[1] = user;
                } else {
                    friend[1] = friendi.get();
                    friend[0] = user;
                }
                IntArrayWritable friendVal = new IntArrayWritable();
                friendVal.set(friend);
                context.write(friendVal, new IntWritable(-1));
                for(String friendjStr: friendsStr) {
                    if(friendiStr.equals(friendjStr))
                        continue;
                    IntWritable friendj = new IntWritable(Integer.parseInt(friendjStr));
                    if(friendi.get() < friendj.get()) {
                        friend[0] = friendi.get();
                        friend[1] = friendj.get();
                    } else {
                        friend[1] = friendi.get();
                        friend[0] = friendj.get();
                    }

                    IntArrayWritable val = new IntArrayWritable();
                    val.set(friend);
                    System.out.println(val);
                    context.write(val, ONE);
                }
            }
        }
    }
    public static class IntArrayWritable implements Writable {
        private int[] data;
        public IntArrayWritable() {
            this.data = new int[0];
        }
        public void set(int[] data) {
            this.data = data;
        }
        public int[] getData() {
            return this.data;
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(data.length);
            for(int i = 0; i < data.length; i++) {
                out.writeInt(data[i]);
            }
        }

        public void readFields(DataInput in) throws IOException {
            int length = in.readInt();

            data = new int[length];

            for(int i = 0; i < length; i++) {
                data[i] = in.readInt();
            }
        }

        public String toString() {
            if(this.data.length == 0) {
                return "[]";
            }
            else if(this.data.length > 0)
                return "fuck you";
            String result = "[";
            for(int i = 0; i < this.data.length - 1; i++) {
                result += this.data[i] + ", ";
            }
            result += this.data[this.data.length - 1] + "]";
            return result;
        }
    }

    public static class Reduce extends Reducer<IntArrayWritable, IntWritable , IntArrayWritable, IntWritable > {
        @Override
        public void reduce(IntArrayWritable key, Iterable<IntWritable> values, Context context)
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