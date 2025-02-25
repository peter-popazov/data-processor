package org.peter.processor.io.importer;


import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Interface for importing trade data in different formats.
 */
public interface TradeImporter {

    /**
     * Imports trade data from a given string.
     *
     * @param inputStream The input stream of data to be processed
     * @return A stream of maps, where each map represents a trade record with column names as keys.
     */
    Stream<Map<String, String>> importData(InputStream inputStream);

    /**
     * Returns the type of export format (e.g., "csv", "json", "xml").
     *
     * @return the export format type as a string
     */
    String getType();
}
