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
import org.apache.hadoop.io.*;
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
    static String clustFile;
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
        job.setOutputKeyClass(DoubleArrayWritable.class);
        job.setOutputValueClass(DoubleArrayWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class); // breaks into lines
        job.setOutputFormatClass(TextOutputFormat.class);

        Vector<Vector<Float>> keys = new Vector<Vector<Float>>();
        BufferedReader br = new BufferedReader(new FileReader(args[2]));
        clustFile = args[2];
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
        File file = new File(args[2]);
        file.delete();
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path temp = new Path("tmp/", UUID.randomUUID().toString());
        ObjectOutputStream os = new ObjectOutputStream(fs.create(temp));
        os.writeObject(keys);
        os.close();
        fs.deleteOnExit(temp);
        DistributedCache.addCacheFile(new URI(temp + "#centroids"), conf);
        DistributedCache.createSymlink(conf);
        for(int i = 0; i < 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }

        return 0;
    }
    // output is centroid and the point so that the reducer gets a centroid and the list of its points
    public static class Map extends Mapper<LongWritable, Text, DoubleArrayWritable, DoubleArrayWritable > {
        static private Vector<Vector<Float>> keys = new Vector<Vector<Float>>();

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            try {
                keys.clear();
                Path uri = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];

                ObjectInputStream os = new ObjectInputStream(new FileInputStream(uri.toString()));
                try {
                    keys = (Vector<Vector<Float>>) os.readObject();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Vector<Float> vec = new Vector<Float>();
            String[] lineArr = value.toString().split(" ");
            for(String s : lineArr) {
                float f = Float.parseFloat(s);
                vec.addElement(f);
            }
            Vector<Float> centroid = keys.get(0);
            Double closest = Double.MAX_VALUE;
            for(Vector<Float> c : keys) {
                double distSq = 0.0;
                for(int i = 0; i < c.size(); i++) {
                    distSq += (c.get(i) - vec.get(i))*(c.get(i) - vec.get(i));
                }
                if(distSq < closest) {
                    closest = distSq;
                    centroid = c;
                }
            }
            DoubleArrayWritable v = new DoubleArrayWritable(vec);
            DoubleArrayWritable k = new DoubleArrayWritable(centroid);
            context.write(k, v);
        }
    }
    public static class DoubleArrayWritable implements Writable,WritableComparable<DoubleArrayWritable> {
        private double[] data;
        public DoubleArrayWritable() {
            this.data = new double[0];
        }
        public DoubleArrayWritable(double[] data) {
            this.data = data;
        }

        public DoubleArrayWritable(Vector<Float> vec) {
            data = new double[vec.size()];
            for(int i = 0 ; i < vec.size(); i++) {
                data[i] = vec.get(i);
            }
        }

        public void set(double[] data) {
            this.data = data;
        }
        public double[] getData() {
            return this.data;
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(data.length);
            for(int i = 0; i < data.length; i++) {
                out.writeDouble(data[i]);
            }
        }

        public void readFields(DataInput in) throws IOException {
            int length = in.readInt();

            data = new double[length];

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

        @Override
        public int compareTo(DoubleArrayWritable o) {
            if(o.getData().length != this.getData().length) {
                return o.getData().length - this.getData().length;
            }
            for(int i = 0; i < o.getData().length; i++) {
                if(o.getData()[i] != this.getData()[i]) {
                    return (int)Math.round(o.getData()[i]) - (int)Math.round(this.getData()[i]);
                }
            }
            return 0;
        }
    }



    public static class Reduce extends Reducer<DoubleArrayWritable, DoubleArrayWritable , DoubleArrayWritable, DoubleArrayWritable > {
        @Override
        public void reduce(DoubleArrayWritable key, Iterable<DoubleArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            double[] newCenter = new double[key.getData().length];
            int j = 0;
            Vector<DoubleArrayWritable> daws = new Vector<DoubleArrayWritable>();
            for(DoubleArrayWritable daw : values) {
                j++;
                double[] pt = daw.getData();
                for(int i = 0; i < pt.length; i++) {
                    newCenter[i] += pt[i];
                }
                daws.addElement(daw);
            }
            for(int i = 0; i < newCenter.length; i++) {
                newCenter[i] = newCenter[i] / j;
            }
            DoubleArrayWritable writableCenter = new DoubleArrayWritable(newCenter);
            for(DoubleArrayWritable daw: daws) {
                context.write(daw, writableCenter);
            }

            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(clustFile, true)));
            for(int i = 0; i < newCenter.length - 1; i++) {
                out.print(newCenter[i]+" ");
                System.out.print(newCenter[i]+" ");
            }
            out.println(newCenter[newCenter.length-1]);
            System.out.println(newCenter[newCenter.length-1]);
            out.close();
        }
    }
}