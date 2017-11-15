import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.SparkException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Created by Madis-Karli Koppel on 2/11/2017.
 */
public class TikaReaderTest {

    @Before
    public void setup() {
        System.setProperty("spark.master", "local[2]");
    }

    // Test if the program shows message about parameters
    //
    @Test(expected = IOException.class)
    public void noArgs() throws IOException {
        String[] args = new String[0];
        TikaReader.main(args);
    }

    //  Program should fail with null args
    @Test(expected = NullPointerException.class)
    public void noPaths() throws IOException {
        TikaReader.main(new String[2]);
    }

    // TODO Tests that actually read files
    // Test PDF
    // Test txt
    // Test docx
    // Test Excel
}
