import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PurchaseMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text customerId = new Text();
    private Text invoiceNo = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Use regex to split while accounting for quoted fields
        String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        try {
            String invoice = fields[0].trim(); // InvoiceNo is column 0
            String customer = fields[6].trim(); // CustomerID is column 6

            // Validate InvoiceNo and CustomerID
            if (!invoice.matches("\\d+") || customer.isEmpty()) {
                return;
            }

            invoiceNo.set(invoice);
            customerId.set(customer);

            context.write(customerId, invoiceNo);
        } catch (Exception e) {
            // Skip malformed rows
        }
    }
}
