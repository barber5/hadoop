package co.brbr5.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendRec extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new FriendRec(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        Job job = new Job(getConf(), "FriendRec");
        job.setJarByClass(FriendRec.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
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
    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntArrayWritable > {
        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            for (String token: value.toString().split("\\s+")) {
                word.set(token);
                int[] arr = {1};
                IntArrayWritable vec = new IntArrayWritable();
                vec.set(arr);
                context.write(ONE, vec);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntArrayWritable, IntWritable, IntArrayWritable >{
        @Override
        public void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            int[] arr = {0};
            IntArrayWritable vec = new IntArrayWritable();
            vec.set(arr);
            context.write(key, vec);
        }
    }
}