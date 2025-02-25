package org.peter.processor.exporter;

public enum ExportType {

    XML("xml"),
    JSON("json"),
    CSV("csv");

    private final String type;

    ExportType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
