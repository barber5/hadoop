package co.brbr5.app;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.io.Writable;
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
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class); // breaks into lines
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, IntWritable, TupleWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
         int user = Integer.parseInt(value.toString().split("\t")[0]);
         String[] friendsStr = value.toString().split("\t")[1].split(",");
         for(String friendiStr: friendsStr) {
            IntWritable friendi = new IntWritable(Integer.parseInt(friendiStr));
            for(String friendjStr: friendsStr) {
               if(friendiStr.equals(friendjStr))
                  continue;
               IntWritable friendj = new IntWritable(Integer.parseInt(friendjStr));
               
               Writable[] val = {friendj, ONE};
               
               context.write(friendi, new TupleWritable(val));
            }
         }
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

   public static class Reduce extends Reducer<IntWritable, TupleWritable, IntWritable, Iterable<IntWritable> > {
      @Override
      public void reduce(IntWritable key, Iterable<TupleWritable> values, Context context)
              throws IOException, InterruptedException {
         HashMap<Integer, FriendCount> counts = new HashMap<Integer, FriendCount>();
         HashMap<Integer, Boolean> ignoreList = new HashMap<Integer, Boolean>(); // I hate java
         for(TupleWritable tw: values) { // count our mutual friends
            IntWritable candWrite = (IntWritable) tw.get(0);
            IntWritable candCount = (IntWritable) tw.get(1);
            int candidate = candWrite.get();
            int cnt = candCount.get();
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
         PriorityQueue<FriendCount> pq = new PriorityQueue<FriendCount>(counts.size(), new FriendComp());
         for(Integer candidate: counts.keySet()) {
            pq.add(counts.get(candidate));
         }

         int i = 0;
         Vector<IntWritable> vals = new Vector<IntWritable>();
         while(i < 10) {
            FriendCount friendSuggestion = pq.poll();
            if(friendSuggestion == null)
               break;
            vals.add(new IntWritable(friendSuggestion.friendId));
            i++;
         }
         context.write(key, vals);
      }
   }
}