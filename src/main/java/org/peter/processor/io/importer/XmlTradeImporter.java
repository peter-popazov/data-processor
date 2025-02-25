package org.peter.processor.io.importer;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.extern.slf4j.Slf4j;
import org.peter.processor.io.ProcessType;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Service
public class XmlTradeImporter implements TradeImporter {
    private final XmlMapper xmlMapper = new XmlMapper();

    @Override
    public Stream<Map<String, String>> importData(InputStream inputStream) {
        return StreamSupport.stream(new XmlTradeSpliterator(inputStream, xmlMapper), false);
    }

    @Override
    public String getType() {
        return ProcessType.XML.getType();
    }

    private static class XmlTradeSpliterator extends Spliterators.AbstractSpliterator<Map<String, String>> {
        private final MappingIterator<Map<String, String>> iterator;

        public XmlTradeSpliterator(InputStream inputStream, XmlMapper xmlMapper) {
            super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL);
            try {
                iterator = xmlMapper.readerFor(Map.class)
                        .readValues(inputStream);
            } catch (Exception e) {
                log.error("Error parsing XML {}", e.getMessage());
                throw new RuntimeException("Error parsing XML", e);
            }
        }

        @Override
        public boolean tryAdvance(Consumer<? super Map<String, String>> action) {
            if (!iterator.hasNext()) {
                return false;
            }
            action.accept(iterator.next());
            return true;
        }
    }
}