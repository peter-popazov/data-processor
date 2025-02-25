package org.peter.processor.io.exporter;

import java.util.List;
import java.util.Map;

/**
 * Interface for exporting trade data in different formats.
 */
// todo enable processing for big files
public interface TradeExporter {

    /**
     * Exports a list of trades into a specific format.
     *
     * @param trades the list of trade data, where each trade is represented as a map of key-value pairs
     * @return a string representation of the exported trades in the corresponding format
     */
    String export(List<Map<String, String>> trades);

    /**
     * Returns the type of export format (e.g., "csv", "json", "xml").
     *
     * @return the export format type as a string
     */
    String getType();
}
