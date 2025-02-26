package org.peter.processor.io.exporter;

import org.peter.processor.io.ProcessType;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public class CsvTradeExporter implements TradeExporter {

    public void writeTrades(BufferedWriter writer, List<Map<String, String>> trades) throws IOException {
        writer.write("date,productName,currency,price\n");
        for (Map<String, String> trade : trades) {
            writer.write(String.join(",",
                    trade.get("date"),
                    trade.get("productName"),
                    trade.get("currency"),
                    trade.get("price")
            ) + "\n");
        }
        writer.flush();
    }

    @Override
    public String getType() {
        return ProcessType.CSV.getType();
    }
}