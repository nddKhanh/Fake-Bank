package com.example.wallet.api;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestControllerAdvice
public class ApiErrors {
    @ExceptionHandler(NotImplemented.class)
    public ResponseEntity<Map<String,String>> missing(NotImplemented error) {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(Map.of(
            "code","NOT_IMPLEMENTED", "message","Backend chưa triển khai. " + error.getMessage()));
    }
}
