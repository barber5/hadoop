package co.brbr5.app;
import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
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

        Vector<Vector<Float>> keys = new Vector<Vector<Float>>();
        BufferedReader br = new BufferedReader(new FileReader(args[2]));
        String line = br.readLine();
        while(line != null) {
            Vector<Float> vec = new Vector<Float>();
            String[] lineArr = line.split(" ");
            for(String s : lineArr) {
                float f = Float.parseFloat(s);
                vec.addElement(f);
            }
            keys.addElement(vec);
            line = br.readLine();
        }
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path temp = new Path("tmp/", UUID.randomUUID().toString());
        ObjectOutputStream os = new ObjectOutputStream(fs.create(temp));
        os.writeObject(keys);
        os.close();
        fs.deleteOnExit(temp);
        DistributedCache.addCacheFile(new URI(temp + "#centroids"), conf);
        DistributedCache.createSymlink(conf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        return 0;
    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntArrayWritable > {
        static private Set<String> keys;

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            try {
                Path uri = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
                System.out.println(uri.toString());
                BufferedReader br = new BufferedReader(new FileReader(uri.toString()));
                for(int i = 0; i < 20; i++) {
                    System.out.println(i);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println(keys.toString());
            context.write(new IntWritable(22), new IntArrayWritable());

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
            context.write(new IntWritable(44), new IntArrayWritable());
        }
    }
}