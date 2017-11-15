import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ExceptionFailure;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Madis-Karli Koppel on 1/11/2017.
 */

public class TikaReader {
    private static final Logger logger = LogManager.getLogger(TikaReader.class);

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.err.println("Usage: TikaReader <input folder> <output folder>");
            throw new IOException("Wrong input arguments");
        }

        String inputPath = args[0];
        String outputPath = args[1];

        sparkTikaReader(inputPath, outputPath);

    }

    public static void sparkTikaReader(String inputPath, String outputPath) {
        // Initialise Spark
        SparkConf sparkConf = new SparkConf().setAppName("Spark PDF text extraction");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        // Read the directory, as (filename, binarycontents) RDD
        JavaPairRDD<String, PortableDataStream> dirContents = sc.binaryFiles(inputPath);

        // Read files and extract text
        // TODO maybe add a filter here to filter file types?
        JavaRDD<Tuple2<String, String>> files = dirContents.map(
                new Function<Tuple2<String, PortableDataStream>, Tuple2<String, String>>() {
                    public Tuple2<String, String> call(Tuple2<String, PortableDataStream> s) {
                        try {

                            logger.debug("reading " + s._1);

                            AutoDetectParser parser = new AutoDetectParser();
                            BodyContentHandler handler = new BodyContentHandler();
                            Metadata metadata = new Metadata();

                            InputStream stream = new ByteArrayInputStream(s._2.toArray());
                            parser.parse(stream, handler, metadata);

                            logger.debug("finished " + s._1);

                            return new Tuple2<String, String>(s._1, handler.toString());
                        } catch (TikaException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (org.xml.sax.SAXException e) {
                            e.printStackTrace();
                        }

                        logger.error("FAILED " + s._1);

                        return new Tuple2<String, String>(s._1, "FAILED");
                    }
                });

        files.saveAsTextFile(outputPath);
        // files.saveAsNewAPIHadoopFile(args[1], Text.class, Text.class, TextOutputFormat.class);

        System.out.println(files.collect());
    }
}
