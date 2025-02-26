package org.peter.processor.io.exporter;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.extern.slf4j.Slf4j;
import org.peter.processor.io.ProcessType;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class XmlTradeExporter implements TradeExporter {
    private final XmlMapper xmlMapper = new XmlMapper();

    @Override
    public void writeTrades(BufferedWriter writer, List<Map<String, String>> trades) throws IOException {
        writer.write("<Trades>\n");

        for (Map<String, String> trade : trades) {
            try {
                Trade tradeWrapper = new Trade(trade);
                writer.write(xmlMapper.writerWithDefaultPrettyPrinter().writeValueAsString(tradeWrapper) + "\n");
            } catch (JsonProcessingException e) {
                log.error("Error converting trade to XML: {}", e.getMessage());
                throw new RuntimeException("Error converting trade to XML", e);
            }
        }

        writer.write("</Trades>\n");
        writer.flush();
    }

    @Override
    public String getType() {
        return ProcessType.XML.getType();
    }

    @JacksonXmlRootElement(localName = "Trade")
    private static class Trade {

        @JacksonXmlProperty(localName = "date")
        private final String date;

        @JacksonXmlProperty(localName = "price")
        private final String price;

        @JacksonXmlProperty(localName = "currency")
        private final String currency;

        @JacksonXmlProperty(localName = "productName")
        private final String productName;

        public Trade(Map<String, String> properties) {
            this.date = properties.getOrDefault("date", "");
            this.productName = properties.getOrDefault("productName", "");
            this.currency = properties.getOrDefault("currency", "");
            this.price = properties.getOrDefault("price", "");
        }
    }

}