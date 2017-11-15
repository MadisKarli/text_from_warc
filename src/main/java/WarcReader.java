import com.google.protobuf.TextFormat;
import nl.surfsara.warcutils.WarcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcRecord;
import org.uncommons.maths.statistics.DataSet;
import org.xml.sax.SAXException;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Madis-Karli Koppel on 3/11/2017.
 */
public class WarcReader {

    private static final Logger logger = LogManager.getLogger(WarcReader.class);

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.err.println("Usage: TikaReader <input folder> <output folder>");
            throw new IOException("Wrong input arguments");
        }

        String inputPath = args[0];
        // TODO remove before prod
        String outputPath = args[1] + System.currentTimeMillis();

        sparkWarcReader(inputPath, outputPath);

        //logger.warn("Starting up...");
        //readHbase();

    }

    private static void sparkWarcReader(String inputPath, final String outputPath) {
        // Initialise Spark
        SparkConf sparkConf = new SparkConf().setAppName("Spark PDF text extraction");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        Configuration hadoopconf = new Configuration();


        //Read in all warc records
        JavaPairRDD<LongWritable, WarcRecord> warcRecords = sc.newAPIHadoopFile(inputPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);


        // TODO maybe combine url and date?
        JavaRDD<Row> records = warcRecords
                // TODO filter HTML out
                .filter(new Function<Tuple2<LongWritable, WarcRecord>, Boolean>() {
                    public Boolean call(Tuple2<LongWritable, WarcRecord> s) {

                        String header = s._2.getHeader("Content-Type").value;

                        if (header.equals("application/warc-fields")) {
                            // Ignore WARC file information
                            return false;
                        }

                        if (header.equals("text/dns")) {
                            // Nothing interesting in DNS files
                            return false;
                        }

                        return true;
                    }
                })
                .map(new Function<Tuple2<LongWritable, WarcRecord>, Row>() {
                    public Row call(Tuple2<LongWritable, WarcRecord> s) throws IOException {
                        String exceptionMessage = "";
                        String exceptionCause = "";
                        logger.debug("reading " + s._1);

                        String id = s._2.getHeader("WARC-Target-URI").value;

                        try {
                            AutoDetectParser parser = new AutoDetectParser();
                            // Minus 1 sets the limit to unlimited
                            // This is needed for bigger files
                            BodyContentHandler handler = new BodyContentHandler(-1);
                            Metadata metadata = new Metadata();

                            parser.parse(s._2.getPayload().getInputStream(), handler, metadata);

                            // TODO can this be done in filter?
                            // Ignore html files as they are handled in another part of the pipeline
                            if(metadata.get("Content-Type").contains("text/html")){
                                logger.debug("IGNORE HTML " + id);
                                return RowFactory.create(id, "ERROR: html document should be handled elsewhere");
                            }

                        // Test if we can recreate the file
                            byte[] buffer = new byte[s._2.getPayload().getInputStream().available()];
                            s._2.getPayload().getInputStream().read(buffer);

                            File targetFile = new File(outputPath +"/"+ System.currentTimeMillis());
                            OutputStream outStream = new FileOutputStream(targetFile);
                            outStream.write(buffer);
//                            byte data[] = s._2.getPayload().getInputStream();
//                            FileOutputStream out = new FileOutputStream(outputPath +"/"+ System.currentTimeMillis());
//                            out.write(bytes);
//                            out.close();

                            logger.debug("finished " + s._1);
                            return RowFactory.create(id, handler.toString());

                        } catch (TikaException e) {
                            // e.printStackTrace();
                            exceptionMessage = e.getMessage();
                            exceptionCause = e.getCause().toString();
                            logger.error(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        } catch (SAXException e) {
                            // e.printStackTrace();
                            exceptionMessage = e.getMessage();
                            exceptionCause = e.getException().toString();
                            logger.error(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        }

                        return RowFactory.create(id, "ERROR: " + exceptionMessage + ":" + exceptionCause + " | " + Arrays.asList(s._2.getHeaderList()));
                    }
                });

        // output.saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class, org.apache.hadoop.mapred.TextOutputFormat.class, hadoopconf);

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("content", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        DataFrame df = sqlContext.createDataFrame(records, schema);

        // records.collect();
        df.write().json(outputPath);

        sc.close();
    }

    public static void readHbase() throws IOException {
        // https://community.hortonworks.com/articles/2038/how-to-connect-to-hbase-11-using-java-apis.html
        logger.info("Setting up hbase configuration");
        TableName tableName = TableName.valueOf("stock-prices");

        Configuration conf = HBaseConfiguration.create();
        // Clientport 2181, but docker changes it
        conf.set("hbase.zookeeper.property.clientPort", "32779");
        conf.set("hbase.zookeeper.quorum", "172.17.0.2");
        conf.set("zookeeper.znode.parent", "/hbase");
        logger.info("Connecting to hbase");
        Connection conn = ConnectionFactory.createConnection(conf);
        logger.info("Connected");
        Admin admin = conn.getAdmin();
//        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor("cf")));
//        }
        logger.info("Inserting into table");
        Table table = conn.getTable(tableName);
        Put p = new Put(Bytes.toBytes("AAPL10232015"));
        p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("close"), Bytes.toBytes(119));
        table.put(p);
        logger.info("Inserted");

        logger.info("Reading from table");
        Result r = table.get(new Get(Bytes.toBytes("AAPL10232015")));
        System.out.println(r);
    }
}


