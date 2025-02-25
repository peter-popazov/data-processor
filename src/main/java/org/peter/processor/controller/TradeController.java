package org.peter.processor.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.peter.processor.exception.UnsupportedFormatException;
import org.peter.processor.io.ProcessType;
import org.peter.processor.service.TradeProcessor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1")
@Tag(name = "Trade API", description = "Endpoints for processing and exporting trade data")
public class TradeController {

    private final TradeProcessor tradeService;

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
    @PostMapping("/process")
    public ResponseEntity<String> exportTrades(
            @RequestHeader(value = HttpHeaders.ACCEPT, defaultValue = "text/csv") String acceptHeader,
            @RequestParam("file") MultipartFile file
    ) {

        String enrichedData;
        try {
            enrichedData = tradeService.processTrades(file.getInputStream(), acceptHeader);
        } catch (UnsupportedFormatException e) {
            return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE).body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Error processing trades: " + e.getMessage());
        }

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=trades." +
                        ProcessType.fromMimeType(acceptHeader).getType())
                .contentType(MediaType.parseMediaType(acceptHeader))
                .body(enrichedData);
    }
}
