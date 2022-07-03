import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.record.Country;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;

import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;

public class Main2 {


    //create the first DoFn
    static class TestMyFirstDoFn extends DoFn<String, String> {
        public static HashMap<String, Integer> countryClicks = new HashMap<>();

        private static final File database = new File("C:\\Users\\Jaafar1018\\IdeaProjects\\cxpipeline\\Files\\GeoLite2-Country.mmdb");
        static DatabaseReader reader ;

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> out) {
            JSONObject myJson = new JSONObject(line);
            String ipAddressString = myJson.getJSONObject("httpRequest").getString("remoteIp");
            String countryName = "";
            try {
            reader = new DatabaseReader.Builder(database).build();


            InetAddress ipAddress = InetAddress.getByName(ipAddressString);

            Country country = reader.country(ipAddress).getCountry();
            countryName = country.getName();
            System.out.println(country.getIsoCode());

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            if (countryClicks.containsKey(countryName)) {
                int currentClicks = countryClicks.get(countryName);
                countryClicks.replace(countryName, currentClicks + 1);
            } else {
                countryClicks.put(countryName, 1);
            }
            out.output(countryName);
        }

    }

    public static void main(String[] args) {

//        PCollection<String> words =

        //create the pipeline options and create the pipeline based on those options
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        //create a PCollection from the requests file
//        PCollection<String> lines = p.apply(TextIO.read().from("C:\\Users\\Jaafar1018\\IdeaProjects\\cxpipeline\\requests.json")).apply(ParDo.of(new TestMyFirstDoFn()));
        PCollection<String> lines = p.apply(TextIO.read().from("C:\\Users\\Jaafar1018\\IdeaProjects\\cxpipeline\\requests.json"));
        lines.apply(ParDo.of(new TestMyFirstDoFn()));
        lines.apply("write to text", TextIO.write().to("C:\\Users\\Jaafar1018\\IdeaProjects\\cxpipeline\\output.txt").withSuffix(".txt"));
//        lines.apply(GroupByKey.<String>create());

        p.run().waitUntilFinish();
    }
}
