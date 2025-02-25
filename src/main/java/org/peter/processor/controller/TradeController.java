package org.peter.processor.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.peter.processor.exception.UnsupportedFormatException;
import org.peter.processor.io.ProcessType;
import org.peter.processor.service.TradeProcessor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;

@Slf4j
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
    public void exportTrades(
            HttpServletResponse response,
            @RequestHeader(value = HttpHeaders.ACCEPT, defaultValue = "text/csv") String acceptHeader,
            @RequestParam("file") MultipartFile file
    ) {
        response.setContentType(acceptHeader);
        response.setHeader("Content-Disposition", "attachment; filename=trades." + ProcessType.fromMimeType(acceptHeader).name().toLowerCase());

        try (InputStream inputStream = file.getInputStream();
             OutputStream outputStream = response.getOutputStream();
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {

            tradeService.processTrades(inputStream, acceptHeader, writer);

            writer.flush();

        } catch (UnsupportedFormatException e) {
            response.setStatus(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
            try {
                response.getWriter().write(e.getMessage());
            } catch (IOException ex) {
                log.error("Error writing error response", ex);
            }
        } catch (Exception e) {
            response.setStatus(HttpStatus.BAD_REQUEST.value());
            try {
                response.getWriter().write("Error processing trades: " + e.getMessage());
            } catch (IOException ex) {
                log.error("Error writing error response", ex);
            }
        }
    }

}