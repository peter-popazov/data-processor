package org.peter.processor.io;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum ProcessType {
    XML("application/xml"),
    JSON("application/json"),
    CSV("text/csv");

    private static final Map<String, ProcessType> lookup = Arrays.stream(values())
            .collect(Collectors.toMap(ProcessType::getType, Function.identity()));

    private final String type;

    ProcessType(String mimeType) {
        this.type = mimeType;
    }

    public String getType() {
        return type;
    }

    public static ProcessType fromMimeType(String mimeType) {
        return lookup.get(mimeType.toLowerCase());
    }
}
