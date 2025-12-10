import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PrecipitationMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // Skip header row
        if (fields[1].equalsIgnoreCase("date")) {
            return;
        }

        try {
            String date = fields[1]; // format: dd/mm/yyyy
            String[] parts = date.split("/");
            String monthYear = parts[1] + "/" + parts[2]; // MM/YYYY

            float precipitationHours = Float.parseFloat(fields[13]); // precipitation_hours
            context.write(new Text(monthYear), new FloatWritable(precipitationHours));
        } catch (Exception e) {
            // Skip malformed lines safely
        }
    }
}
