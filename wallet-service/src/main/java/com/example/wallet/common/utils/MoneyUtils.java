package com.example.wallet.common.utils;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Chuyển đổi số tiền dùng chung, không phụ thuộc service cụ thể. */
public final class MoneyUtils {

    private MoneyUtils() {
    }

    public static long parseMoney(String value) {
        // API nhận tiền dưới dạng chuỗi; domain dùng long tương ứng BIGINT.
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Money must be an integer within BIGINT range"
            );
        }
    }
}
