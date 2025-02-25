package org.peter.processor.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.peter.processor.service.TradeProcessor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1")
@Tag(name = "Trade API", description = "Endpoints for processing and exporting trade data")
public class TradeController {

    private final TradeProcessor tradeService;

    private static final Map<String, String> formatsMap = Map.of(
            MediaType.APPLICATION_XML_VALUE, "xml",
            MediaType.APPLICATION_JSON_VALUE, "json",
            "text/csv", "csv"
    );


    @Operation(
            summary = "Export Enriched Trades",
            description = "Exports trade data in XML, JSON, or CSV format based on the Accept header."
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Successfully exported trades",
                    content = {
                            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(type = "string")),
                            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(type = "string")),
                            @Content(mediaType = "text/csv", schema = @Schema(type = "string"))
                    }),
            @ApiResponse(responseCode = "400", description = "Bad request"),
    })
    @GetMapping("/process")
    public ResponseEntity<String> exportTrades(
            @Parameter(description = "Response format: XML, JSON, or CSV", example = "application/json")
            @RequestHeader(value = HttpHeaders.ACCEPT, defaultValue = "text/csv") String acceptHeader
            // todo accept data to process
    ) {
        String format = formatsMap.getOrDefault(acceptHeader.toLowerCase(), "csv");

        String result;
        try {
            result = tradeService.processTrades("data-sample.csv", format);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=trades." + format)
                .contentType(MediaType.parseMediaType(acceptHeader))
                .body(result);
    }
}
