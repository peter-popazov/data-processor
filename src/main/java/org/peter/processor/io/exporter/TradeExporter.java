package org.peter.processor.io.exporter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for exporting trade data in different formats.
 */
public interface TradeExporter {

    /**
     * Exports a list of trades into a specific format.
     *
     * @param trades the list of stream trade data, where each trade is represented as a map of key-value pairs
     */
    void writeTrades(BufferedWriter writer, List<Map<String, String>> trades) throws IOException;

    /**
     * Returns the type of export format (e.g., "csv", "json", "xml").
     *
     * @return the export format type as a string
     */
    String getType();
}
