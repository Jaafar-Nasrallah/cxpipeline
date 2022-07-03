import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.record.Country;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.HashMap;

@RunWith(JUnit4.class)
public class CxPipelineTest {

    static final String[] COUNTS_ARRAY = new String[]{"Lebanon: 15 requests", "United Arab Emirates: 1 request", "Canada: 2561 requests"};

    //Lebanon: 15 requests
    //United Arab Emirates: 1 requests
    //Canada: 2561 requests
    //create the first DoFn
    static class Tokenize extends DoFn<String, String> {
        public static HashMap<String, Integer> countryClicks = new HashMap<>();

        private static final File database = new File("C:\\Users\\Jaafar1018\\IdeaProjects\\cxpipeline\\Files\\GeoLite2-Country.mmdb");
        static DatabaseReader reader;

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> out) {
            //parse json string into a json object, and get the IP address from the remoteIP property in httpRequest object
            JSONObject myJson = new JSONObject(line);
            String ipAddressString = myJson.getJSONObject("httpRequest").getString("remoteIp");
            String countryName = "";
            try {
                //create the Geolite2 country database reader object, and get the country based on the extracted IP address
                reader = new DatabaseReader.Builder(database).build();
                InetAddress ipAddress = InetAddress.getByName(ipAddressString);
                Country country = reader.country(ipAddress).getCountry();
                countryName = country.getName();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            out.output(countryName);
        }

    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue() + (input.getValue() == 1 ? " request" : " requests");
        }
    }

    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new Tokenize()));

            // Count the number of times each word occurs and return the PCollection result.
            return words.apply(Count.perElement());

        }
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testCxPipeline() {

        //create the pipeline options and create the pipeline based on those options
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = TestPipeline.create(options);


        String currentWorkingDir = Paths.get("").toAbsolutePath().normalize().toString();
        //use the test json file
        String requestFilePath = currentWorkingDir + "\\requests2.json";
        //create a PCollection from the requests file
        PCollection<String> lines = p.apply(TextIO.read().from(requestFilePath));
        //transform the lines of the requests, store into countryNames PCollection
        PCollection<KV<String, Long>> countryNames = lines.apply(new CountWords());
        PCollection<String> countryNameRequests = countryNames.apply(MapElements.via(new CxPipeline.FormatAsTextFn()));
        PAssert.that(countryNameRequests).containsInAnyOrder(COUNTS_ARRAY);

        p.run();
    }
}
