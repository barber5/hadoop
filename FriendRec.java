package co.brbr5.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Comparator;
import java.util.PriorityQueue;
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

        job.setInputFormatClass(TextInputFormat.class); // breaks into lines
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("lovely lovely config");

        job.waitForCompletion(true);

        return 0;
    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntArrayWritable > {
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
                int[] friend = {friendi.get(), -1};
                IntArrayWritable friendVal = new IntArrayWritable();
                friendVal.set(friend);
                context.write(new IntWritable(user), friendVal);
                for(String friendjStr: friendsStr) {
                    if(friendiStr.equals(friendjStr))
                        continue;
                    IntWritable friendj = new IntWritable(Integer.parseInt(friendjStr));
                    int[] arr = {friendj.get(), 1};
                    IntArrayWritable val = new IntArrayWritable();
                    val.set(arr);
                    context.write(friendi, val);
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
            String result = "[";
            for(int i = 0; i < this.data.length - 1; i++) {
                result += this.data[i] + ", ";
            }
            result += this.data[this.data.length - 1] + "]";
            return result;
        }
    }

    public static class FriendCount {
        public int friendId;
        public int count;
        public FriendCount(int friendId) {
            this.friendId = friendId;
            this.count = 0;
        }
    }

    public static class FriendComp implements Comparator<FriendCount> {
        @Override
        public int compare(FriendCount f1, FriendCount f2) {
            return f1.count - f2.count;
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntArrayWritable , IntWritable, IntArrayWritable > {
        @Override
        public void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            HashMap<Integer, FriendCount> counts = new HashMap<Integer, FriendCount>();
            HashMap<Integer, Boolean> ignoreList = new HashMap<Integer, Boolean>(); // I hate java
            for(IntArrayWritable tw: values) { // count our mutual friends
                int[] data = tw.getData();
                int candidate =  data[0];
                int cnt =  data[1];
                if(cnt < 0) {
                    ignoreList.put(candidate, true);
                    counts.remove(candidate);
                }
                if(ignoreList.containsKey(candidate)) { // already a friend
                    continue;
                }

                if(!counts.containsKey(candidate)) {
                    counts.put(candidate, new FriendCount(candidate));
                }
                counts.get(candidate).count += 1;
            }
            PriorityQueue<FriendCount> pq = new PriorityQueue<FriendCount>(20, new FriendComp());
            for(Integer candidate: counts.keySet()) {
                pq.add(counts.get(candidate));
            }

            int i = 0;
            int[] vals = new int[10];
            while(i < 10) {
                FriendCount friendSuggestion = pq.poll();
                if(friendSuggestion == null)
                    break;
                vals[i] = friendSuggestion.friendId;
                i++;
            }
            IntArrayWritable valsWritable = new IntArrayWritable();
            valsWritable.set(vals);
            context.write(key, valsWritable);
        }
    }
}