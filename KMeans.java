package co.brbr5.app;
import java.io.*;
import java.util.Arrays;
import java.util.Set;

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

public class KMeans extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new KMeans(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        Job job = new Job(getConf(), "KMeans");
        job.setJarByClass(KMeans.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class); // breaks into lines
        job.setOutputFormatClass(TextOutputFormat.class);

        for(int i = 0; i < 20; i++) {
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }
        return 0;
    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntArrayWritable > {
        static private Set<String> keys;

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            ObjectInputStream is =
                    null;
            try {
                is = new ObjectInputStream(new FileInputStream("c1.txt"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                keys = (Set<String>) is.readObject();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println(keys.toString());


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
            String result = "[";
            for(int i = 0; i < this.data.length - 1; i++) {
                result += this.data[i] + ", ";
            }
            result += this.data[this.data.length - 1] + "]";
            return result;
        }
    }



    public static class Reduce extends Reducer<IntWritable, IntArrayWritable , IntWritable, IntArrayWritable > {
        @Override
        public void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context)
                throws IOException, InterruptedException {

        }
    }
}