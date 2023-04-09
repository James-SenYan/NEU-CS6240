package MajorTask2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Reduce-side join
 */
public class RentalActivityEachDayWithWeather {

  private static final String HBASE_TABLE_NAME = "BikeData";

  public static class RentalActivityMapper extends Mapper<Object, Result, Text, IntWritable>{
    private HashMap<String, Integer> counter;
    private Text date = new Text();
    @Override
    public void setup(Context context){
      counter = new HashMap<>();
    }
    @Override
    public void map(Object key, Result value, Context context){
      byte[] s = value.getValue("Bike".getBytes(), "startTime".getBytes());
      String str = new String(s, StandardCharsets.UTF_8);
      StringTokenizer itr = new StringTokenizer(str);
      while(itr.hasMoreTokens()){
        String token = itr.nextToken().toLowerCase();
        token = token.split("#")[0];
        counter.put(token, counter.getOrDefault(token, 0) + 1);
      }
    }

    public void cleanup(Context context
    ) throws IOException, InterruptedException {
      // called at end of the task
      for(String token : counter.keySet()) {
        date.set(token);
        context.write(date, new IntWritable(counter.get(token)));
      }
    }
  }
  //join with weather table
  public static class RentalActivityReducer extends Reducer<Text,IntWritable,Text,Text>{
    private HTable weatherTable;
    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = HBaseConfiguration.create();
      //for aws connection
      //String hbaseconf = "/etc/hbase/conf/hbase-site.xml";
      //conf.addResource(new File(hbaseconf).toURI().toURL());
      Connection conn = ConnectionFactory.createConnection(conf);
      weatherTable = (HTable) conn.getTable(TableName.valueOf("WeatherData"));
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      //join happens here
      Get get = new Get(key.toString().getBytes());
      Result weather = weatherTable.get(get);
      String valueDate = new String(weather.getValue("Weather".getBytes(), "date".getBytes()), StandardCharsets.UTF_8);
      String precipitation = new String(weather.getValue("Weather".getBytes(), "precipitation".getBytes()), StandardCharsets.UTF_8);
      String maxTemperature = new String(weather.getValue("Weather".getBytes(), "maxTemperature".getBytes()), StandardCharsets.UTF_8);
      String minTemperature = new String(weather.getValue("Weather".getBytes(), "minTemperature".getBytes()), StandardCharsets.UTF_8);
      String out = String.join("\t", String.valueOf(sum), precipitation, maxTemperature, minTemperature);
      context.write(key, new Text(out));
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = HBaseConfiguration.create();
    //for aws connection
    //String hbaseconf = "/etc/hbase/conf/hbase-site.xml";
    //conf.addResource(new File(hbaseconf).toURI().toURL());
    Connection conn = ConnectionFactory.createConnection(conf);
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length != 1) {
      System.err.println("Usage: COMPUTE <out-file-path>");
      System.exit(2);
    }
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    Job job = new Job(conf, "Rental Activity");
    job.setInputFormatClass(TableInputFormat.class);
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, HBASE_TABLE_NAME);
    job.getConfiguration().set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
    job.setJarByClass(RentalActivityEachDayWithWeather.class);
    job.setMapperClass(RentalActivityMapper.class);
    job.setReducerClass(RentalActivityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
