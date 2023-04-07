package MajorTask2; /**
 *
 */
import com.opencsv.CSVParser;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class HpopulateBike {

  public static final String HBASE_TABLE_NAME = "BikeData";
  public static final String COLUMN_FAMILY = "Bike";
  public static final String COLUMN_TRIP_DURATION = "tripDuration";
  public static final String COLUMN_START_TIME = "startTime";
  public static final String COLUMN_STOP_TIME = "stopTime";
  public static final String COLUMN_START_STATION_ID = "startStationId";
  public static final String COLUMN_START_STATION_NAME = "startStationName";
  public static final String COLUMN_START_STATION_LATITUDE = "startStationLatitude";
  public static final String COLUMN_START_STATION_LONGITUDE = "startStationLongitude";
  public static final String COLUMN_END_STATION_ID = "endStationId";
  public static final String COLUMN_END_STATION_NAME = "endStationName";
  public static final String COLUMN_END_STATION_LATITUDE = "endStationLatitude";
  public static final String COLUMN_END_STATION_LONGITUDE = "endStationLongitude";
  public static final String COLUMN_BIKE_ID = "bikeID";
  public static final String COLUMN_USERTYPE = "userType";
  public static final String COLUMN_BIRTH_YEAR = "birthYear";
  public static final String COLUMN_GENDER = "gender";

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    // Arguments length check
    if (otherArgs.length != 1) {
      System.err.println("Usage: HPopulate <bike_input-file-path>");
      System.exit(2);
    }
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    // Check if the table already exists
    if (admin.tableExists(TableName.valueOf(HBASE_TABLE_NAME))) {
      admin.disableTable(TableName.valueOf(HBASE_TABLE_NAME));
      admin.deleteTable(TableName.valueOf(HBASE_TABLE_NAME));
      System.out.println("HTable " + HBASE_TABLE_NAME
          + " already exists! Deleting...");
    }
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(HBASE_TABLE_NAME));
    HColumnDescriptor hcd = new HColumnDescriptor(COLUMN_FAMILY);
    htd.addFamily(hcd);
    admin.createTable(htd);
    System.out.println("HBase table " + HBASE_TABLE_NAME
        + " created.");
    Job job = new Job(conf, "HBase table populate");
    job.setJarByClass(HpopulateBike.class);
    job.setMapperClass(HPopulateMapper.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    admin.close();
  }

  //mapper class
  public static class HPopulateMapper extends
      Mapper<Object, Text, ImmutableBytesWritable, Writable> {
    private CSVParser csvParser = null;
    private HTable table = null;

    protected void setup(Context context) throws IOException {
      this.csvParser = new CSVParser();
      Configuration config = HBaseConfiguration.create();
      Connection connection = ConnectionFactory.createConnection(config);
      table = (HTable) connection.getTable(TableName.valueOf(HBASE_TABLE_NAME));
    }

    public void map(Object offset, Text value, Context context)
        throws IOException {
      // Parse the input line
      String[] line = this.csvParser.parseLine(value.toString());
      for(String item : line){
        if(item == null || item.length() == 0) return;
      }
      StringBuilder rowKey = new StringBuilder();
      String tripDuration = line[0];//in seconds
      String startTime = line[1];
      String stopTime = line[2];
      String startStationId = line[3];
      String startStationName = line[4];
      String startStationLatitude = line[5];
      String startStationLongitude = line[6];
      String endStationId = line[7];
      String endStationName = line[8];
      String endStationLatitude = line[9];
      String endStationLongitude = line[10];
      String bikeId = line[11];
      String userType = line[12];
      String birthYear = line[13];
      String gender = line[14];

      LocalDate startDate = LocalDate.parse(startTime, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
      //rowKey.append(startDate.toString());
      rowKey.append(startTime).append("#").append(bikeId);
      Put put = new Put(Bytes.toBytes(rowKey.toString()));
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_TRIP_DURATION.getBytes(), tripDuration.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_START_TIME.getBytes(), startDate.toString().getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_STOP_TIME.getBytes(), stopTime.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_START_STATION_ID.getBytes(), startStationId.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_START_STATION_NAME.getBytes(), startStationName.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_START_STATION_LATITUDE.getBytes(), startStationLatitude.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_START_STATION_LONGITUDE.getBytes(), startStationLongitude.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_END_STATION_ID.getBytes(), endStationId.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_END_STATION_NAME.getBytes(), endStationName.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_END_STATION_LATITUDE.getBytes(), endStationLatitude.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_END_STATION_LONGITUDE.getBytes(), endStationLongitude.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_BIKE_ID.getBytes(), bikeId.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_USERTYPE.getBytes(), userType.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_BIRTH_YEAR.getBytes(), birthYear.getBytes());
      put.addColumn(COLUMN_FAMILY.getBytes(), COLUMN_GENDER.getBytes(), gender.getBytes());
      table.put(put);
    }

    protected void cleanup(Context context) throws IOException {
      table.close();
    }
  }
}

