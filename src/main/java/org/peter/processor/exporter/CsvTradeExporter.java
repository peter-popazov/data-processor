package org.peter.processor.exporter;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class CsvTradeExporter implements TradeExporter {

    @Override
    public String export(List<Map<String, String>> trades) {
        StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append("date,productId,productName,currency,price\n");

        for (Map<String, String> trade : trades) {
            csvBuilder.append(trade.get("date")).append(",");
            csvBuilder.append(trade.get("productName")).append(",");
            csvBuilder.append(trade.get("currency")).append(",");
            csvBuilder.append(trade.get("price")).append("\n");
        }

        return csvBuilder.toString();
    }

    @Override
    public String getType() {
        return ExportType.CSV.name();
    }
}
