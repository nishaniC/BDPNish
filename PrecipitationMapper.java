import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PrecipitationMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");


        // skip header
        if (fields[1].equals("date")) return;

        try {
            String date = fields[1]; // format: dd/mm/yyyy
            String[] parts = date.split("/");
            String monthYear = parts[1] + "/" + parts[2]; // MM/YYYY

            float precipitation = Float.parseFloat(fields[13]); // precipitation_hours
            context.write(new Text(monthYear), new FloatWritable(precipitationHours));
        } catch (Exception e) {
            // skip malformed lines
        }
    }
}