import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MonthlyVisits extends Configured implements Tool {
  public static class VisitMapper
      extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String[] f = line.split("\t", -1);
      if (f.length != 4 || f[0].isEmpty() || f[3].isEmpty()
          || !f[1].matches("[0-9]{4}")
          || !f[2].matches("[1-9]|1[0-2]") || line.contains("\r")) {
        throw new IOException("Invalid benchmark row");
      }
      context.write(new Text(line), ONE);
    }
  }
  public static class SumReducer
      extends Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable v : values) sum += v.get();
      context.write(key, new LongWritable(sum));
    }
  }
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MonthlyVisits <input> <output>");
      return 2;
    }
    Job job = Job.getInstance(getConf(), "Monthly visit counts");
    job.setJarByClass(MonthlyVisits.class);
    job.setMapperClass(VisitMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.out.println("MR_MODE=" + job.getConfiguration()
        .get("mapreduce.framework.name", "local"));
    return job.waitForCompletion(true) ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new MonthlyVisits(), args));
  }
}
