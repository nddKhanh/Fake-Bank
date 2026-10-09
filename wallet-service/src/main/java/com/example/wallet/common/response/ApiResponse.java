package com.example.wallet.common.response;

import java.time.LocalDateTime;

public record ApiResponse<T>(
        int code,
        boolean success,
        String message,
        T data,
        LocalDateTime timestamp
) {
    public static <T> ApiResponse<T> success(String message, T data) {
        return new ApiResponse<>(200, true, message, data, LocalDateTime.now());
    }

    public static <T> ApiResponse<T> success(String message){
        return new ApiResponse<>(200, true, message, null, LocalDateTime.now());
    }

    public static <T> ApiResponse<T> created(String message, T data){
        return new ApiResponse<>(201, true, message, data, LocalDateTime.now());
    }

    public static <T> ApiResponse<T> error(int code, String message){
        return new ApiResponse<>(code, false, message, null, LocalDateTime.now());
    }

    public static <T> ApiResponse<T> error(int code, String message, T data){
        return new ApiResponse<>(code, false, message, data, LocalDateTime.now());
    }
}
