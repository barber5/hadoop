package co.brbr5.app;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new KMeans(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = new Job(new Configuration(), "Kmeans");
        job.setJarByClass(KMeans.class);
        job.setOutputKeyClass(DoubleArrayWritable.class);
        job.setOutputValueClass(DoubleArrayWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class); // breaks into lines
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Vector<Vector<Double>> keys = new Vector<Vector<Double>>();
        BufferedReader br = new BufferedReader(new FileReader(args[2]));
        String line = br.readLine();
        while(line != null) {
            Vector<Double> vec = new Vector<Double>();
            String[] lineArr = line.split(" ");
            for(String s : lineArr) {
                double f = Double.parseDouble(s);
                vec.addElement(f);
            }
            keys.addElement(vec);
            line = br.readLine();
        }
        File file = new File(args[2]);
        file.delete();
        Configuration conf = job.getConfiguration();
        conf.set("centroids", args[2]);
        FileSystem fs = FileSystem.get(conf);
        Path temp = new Path("tmp/", UUID.randomUUID().toString());
        ObjectOutputStream os = new ObjectOutputStream(fs.create(temp));
        os.writeObject(keys);
        os.close();
        fs.deleteOnExit(temp);
        DistributedCache.addCacheFile(new URI(temp + "#centroids"), conf);
        DistributedCache.createSymlink(conf);
        job.waitForCompletion(true);
        for(int i = 0; i < 19; i++) {
            Job job2 = new Job(job.getConfiguration(), "Kmeans");
            job2.setJarByClass(KMeans.class);
            job2.setOutputKeyClass(DoubleArrayWritable.class);
            job2.setOutputValueClass(DoubleArrayWritable.class);

            job2.setMapperClass(Map.class);
            job2.setReducerClass(Reduce.class);
            System.out.println(args[0]);
            System.out.println(args[1]);
            job2.setInputFormatClass(TextInputFormat.class); // breaks into lines
            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+i));
            job2.waitForCompletion(true);
            job = job2;
        }



        return 0;
    }
    // output is centroid and the point so that the reducer gets a centroid and the list of its points
    public static class Map extends Mapper<LongWritable, Text, DoubleArrayWritable, DoubleArrayWritable > {
        static private Vector<Vector<Double>> keys = new Vector<Vector<Double>>();

        public void setup(Context context) {

            Configuration conf = context.getConfiguration();
            try {
                keys.clear();
                Path[] uris = DistributedCache.getLocalCacheFiles(conf);
                for(Path p : uris) {
                    System.out.println(p.toString());
                }
                Path uri = uris[uris.length - 1];

                ObjectInputStream os = new ObjectInputStream(new FileInputStream(uri.toString()));
                try {
                    keys = (Vector<Vector<Double>>) os.readObject();
                    System.out.println("new centroids coming up\n\n\n");
                    for(Vector<Double> vd : keys) {
                        System.out.print("centroid: ");
                        for(Double d : vd) {
                            System.out.print(d+" ");
                        }
                        System.out.println();
                    }
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
            Vector<Double> vec = new Vector<Double>();
            String[] lineArr = value.toString().split(" ");
            for(String s : lineArr) {
                double f = Double.parseDouble(s);
                vec.addElement(f);
            }
            System.out.println("Finding best centroid for point: "+vecStr(vec));
            Vector<Double> centroid = keys.get(0);
            Double closest = Double.MAX_VALUE;
            for(Vector<Double> c : keys) {
                double distSq = 0.0;
                for(int i = 0; i < c.size(); i++) {
                    distSq += (c.get(i) - vec.get(i))*(c.get(i) - vec.get(i));
                }
                if(distSq < closest) {
                    closest = distSq;
                    centroid = c;
                }
            }
            System.out.println("With cost "+closest+" best centroid is "+vecStr(centroid));
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

        public DoubleArrayWritable(Vector<Double> vec) {
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
                data[i] = in.readDouble();
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

    public static String vecStr(Vector<Double> vd) {
        String result = "[ ";
        for(Double d: vd) {
            result += d +" ";
        }
        result += "]";
        return result;
    }

    public static String vecStr(double[] vd) {
        String result = "[ ";
        for(Double d: vd) {
            result += d +" ";
        }
        result += "]";
        return result;
    }



    public static class Reduce extends Reducer<DoubleArrayWritable, DoubleArrayWritable , DoubleArrayWritable, DoubleArrayWritable > {
        static private Vector<Vector<Double>> keys = new Vector<Vector<Double>>();
        static private Vector<Double> costs =  new Vector<Double>();
        static private Vector<Double> totalC =  new Vector<Double>();
        @Override
        public void reduce(DoubleArrayWritable key, Iterable<DoubleArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            double[] newCenter = new double[key.getData().length];
            double cost = 0.0;
            Vector<DoubleArrayWritable> daws = new Vector<DoubleArrayWritable>();
            for(DoubleArrayWritable daw : values) {
                double[] pt = daw.getData();

                //System.out.println("Point: "+vecStr(pt));
                //System.out.println("Centroid: "+vecStr(key.getData()));
                // pt is a data point for this centroid
                for(int i = 0; i < pt.length; i++) {

                    newCenter[i] += pt[i];

                    cost += (pt[i]-key.getData()[i])*(pt[i]-key.getData()[i]);
                }
                daws.addElement(daw);
            }
            costs.addElement(cost);
            Vector<Double> centroid = new Vector<Double>();
            for(int i = 0; i < newCenter.length; i++) {
                if(daws.size() > 0) {
                    newCenter[i] = newCenter[i] / daws.size();
                }
                else {
                    newCenter[i] = 0.0;
                }
                centroid.addElement(newCenter[i]);
            }
            System.out.println("There are  "+daws.size()+" costing a total of "+cost+" in cluster "+vecStr(newCenter));
            DoubleArrayWritable writableCenter = new DoubleArrayWritable(newCenter);
            for(DoubleArrayWritable daw: daws) {
                context.write(daw, writableCenter);
            }
            keys.addElement(centroid);

        }
        @Override
        public void cleanup(Context context) {
            Configuration conf = context.getConfiguration();
            FileSystem fs = null;
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Path temp = new Path("tmp/", UUID.randomUUID().toString());
            ObjectOutputStream os = null;
            try {
                os = new ObjectOutputStream(fs.create(temp));
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                os.writeObject(keys);

                System.out.println("writing centroids\n\n\n");
                for(Vector<Double> vd : keys) {
                    System.out.print("centroid: ");
                    for(Double d : vd) {
                        System.out.print(d+" ");
                    }
                    System.out.println();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                fs.deleteOnExit(temp);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                DistributedCache.addCacheFile(new URI(temp + "#centroids"), conf);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            DistributedCache.createSymlink(conf);
            double totalCost = 0.0;
            for(Double d : costs) {
                totalCost += d;
            }
            totalC.addElement(totalCost);
            for(Double d : totalC) {
                System.out.println(d);
            }
            keys.clear();
            costs.clear();
        }
    }
}