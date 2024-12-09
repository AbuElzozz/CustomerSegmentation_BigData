import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class PurchaseReducer extends Reducer<Text, Text, Text, Text> {
    private Text classification = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueInvoices = new HashSet<>();

        // Collect unique Invoice Numbers
        for (Text invoice : values) {
            uniqueInvoices.add(invoice.toString());
        }

        int purchaseCount = uniqueInvoices.size();

        // Classify based on purchase frequency
        if (purchaseCount < 5) {
            classification.set("Low Frequency");
        } else {
            classification.set("High Frequency");
        }

        // Write output as CustomerID -> Frequency Category
        context.write(key, classification);
    }
}
