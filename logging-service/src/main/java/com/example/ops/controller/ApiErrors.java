package com.example.ops.controller;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;

@RestControllerAdvice
public class ApiErrors {
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String,String>> requestError(ResponseStatusException error) {
        return ResponseEntity.status(error.getStatusCode()).body(Map.of(
                "code", Integer.toString(error.getStatusCode().value()),
                "message", error.getReason() == null ? "Request failed" : error.getReason()
        ));
    }

    @ExceptionHandler(NotImplemented.class)
    public ResponseEntity<Map<String,String>> missing(NotImplemented error) {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(Map.of(
            "code","NOT_IMPLEMENTED", "message","Backend chưa triển khai. " + error.getMessage()));
    }
}

