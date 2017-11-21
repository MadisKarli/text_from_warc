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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jwat.warc.WarcRecord;
import org.xml.sax.SAXException;
import scala.Tuple2;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Created by Madis-Karli Koppel on 3/11/2017.
 * Reads warc files
 * Extracts text from everything that is not HTML
 * HTML text extraction is left for process.py that was better at this
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

        long start = System.currentTimeMillis();
        logger.info("Starting spark...");
        sparkWarcReader(inputPath, outputPath);
        logger.info("Spark Finished...");
        long end = System.currentTimeMillis();
        logger.error("Total time taken " + (end-start));

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


        // Here we create JavaRDD<Row> because they are the easiest to save into hbase
        // If hbase is not used then we could change it to other JavaPairRDD that is easiest to save as sequence file
        JavaPairRDD<Text, Tuple2<Text, Text>> records2 = warcRecords
                .filter(new Function<Tuple2<LongWritable, WarcRecord>, Boolean>() {
                    public Boolean call(Tuple2<LongWritable, WarcRecord> s) throws Exception {
                        String header = s._2.getHeader("Content-Type").value;

                        // Ignore WARC file information
                        if (header.equals("application/warc-fields")) return false;

                        // Nothing interesting in DNS files
                        if (header.equals("text/dns")) return false;

                        return true;
                    }
                }).mapToPair(new PairFunction<Tuple2<LongWritable, WarcRecord>, Text, Tuple2<Text, Text>>() {
                    public Tuple2<Text, Tuple2<Text, Text>> call(Tuple2<LongWritable, WarcRecord> s) throws Exception {
                        String exceptionMessage = "";
                        String exceptionCause = "";
                        String contentType = "";
                        logger.debug("reading " + s._1);

                        // Construct the ID as it was in nutch
                        // http::g.delfi.ee::/s/img/back_grey.gif::null::20150214090921
                        String date = s._2.getHeader("WARC-Date").value;
                        date = date.replaceAll("-|T|Z|:", "");

                        URL url = new URL(s._2.getHeader("WARC-Target-URI").value);
                        String protocol = url.getProtocol();
                        String hostname = url.getHost();
                        String urlpath = url.getPath();
                        String param = url.getQuery();

                        String id = protocol + "::" + hostname + "::" + urlpath + "::" + param + "::" + date;

                        try {
                            AutoDetectParser parser = new AutoDetectParser();
                            // Minus 1 sets the limit to unlimited
                            // This is needed for bigger files
                            BodyContentHandler handler = new BodyContentHandler(-1);
                            Metadata metadata = new Metadata();

                            parser.parse(s._2.getPayload().getInputStream(), handler, metadata);
                            contentType = metadata.get("Content-Type");

                            // Do not process html files, process.py is better at this
                            if (contentType.contains("text/html")) {
                                return new Tuple2<Text, Tuple2<Text, Text>>(new Text(contentType), new Tuple2<Text, Text>(new Text(id), new Text(IOUtils.toString(s._2.getPayload().getInputStream()))));
                            }

                            logger.debug("finished " + s._1);
                            return new Tuple2<Text, Tuple2<Text, Text>>(new Text(contentType), new Tuple2<Text, Text>(new Text(id), new Text(handler.toString())));

                        } catch (TikaException e) {
                            exceptionMessage = e.getMessage();
                            exceptionCause = e.getCause().toString();
                            logger.debug(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        } catch (SAXException e) {
                            exceptionMessage = e.getMessage();
                            exceptionCause = e.getException().toString();
                            logger.debug(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        }

                        // TODO what should we do with the errors?
                        return new Tuple2<Text, Tuple2<Text, Text>>(new Text("ERROR" + contentType), new Tuple2<Text, Text>(new Text(id), new Text("ERROR: " + exceptionMessage + ":" + exceptionCause + " | " + Arrays.asList(s._2.getHeaderList()))));
                    }
                });

        // filter based on file type. Needed for testing and can be removed later on
        // Html and html-like files
        JavaPairRDD<Text, Text> html = records2.filter(new Function<Tuple2<Text, Tuple2<Text, Text>>, Boolean>() {
            public Boolean call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                if (s._1.toString().contains("text/html"))return true;
                if (s._1.toString().contains("application/xhtml+xml"))return true;
                if (s._1.toString().contains("application/xml"))return true;
                if (s._1.toString().contains("application/rss+xml"))return true;
                return false;
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Tuple2<Text, Text>>, Text, Text>() {
            public Tuple2<Text, Text> call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                return s._2;
            }
        });

        // plaintext files, mostly robot responses or javascript code
        JavaPairRDD<Text, Text> plaintext = records2.filter(new Function<Tuple2<Text, Tuple2<Text, Text>>, Boolean>() {
            public Boolean call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                if (s._1.toString().contains("text/plain")) return true;
                return false;
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Tuple2<Text, Text>>, Text, Text>() {
            public Tuple2<Text, Text> call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                return s._2;
            }
        });

        // all other file types
        // includes pdf and word documents
        JavaPairRDD<Text, Text> others = records2.filter(new Function<Tuple2<Text, Tuple2<Text, Text>>, Boolean>() {
            public Boolean call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                if (s._1.toString().contains("text/plain"))return false;
                if (s._1.toString().contains("text/html"))return false;
                if (s._1.toString().contains("application/xhtml+xml"))return false;
                if (s._1.toString().contains("application/xml"))return false;
                if (s._1.toString().contains("application/rss+xml"))return false;
                return true;
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Tuple2<Text, Text>>, Text, Text>() {
            public Tuple2<Text, Text> call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                return s._2;
            }
        });

        html.saveAsNewAPIHadoopFile(outputPath + "/html", Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
        plaintext.saveAsNewAPIHadoopFile(outputPath + "/plaintext", Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
        others.saveAsNewAPIHadoopFile(outputPath + "/others", Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
        ///saveDF(html, sqlContext, outputPath, "html");
        //saveDF(plaintext, sqlContext, outputPath, "plaintext");
        //saveDF(others, sqlContext, outputPath, "others");

        sc.close();
    }

    private static void saveDF(JavaRDD<Row> javaRDD, SQLContext sqlContext, String outputPath, String dirName) {
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("contentType", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("content", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        DataFrame df = sqlContext.createDataFrame(javaRDD, schema);

        // TODO save as into Hbase
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


