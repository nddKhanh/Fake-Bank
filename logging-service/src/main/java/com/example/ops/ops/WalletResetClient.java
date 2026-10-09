package com.example.ops.ops;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/** Forwards the explicitly confirmed local reset to Wallet; this service keeps its JDBC connection read-only. */
@Service
public class WalletResetClient {
    private final RestClient wallet;

    public WalletResetClient(@Value("${wallet.url}") String walletUrl) {
        this.wallet = RestClient.builder().baseUrl(walletUrl).build();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> reset() {
        try {
            Map<String, Object> response = wallet.post()
                    .uri("/api/dev/reset")
                    .body(Map.of("confirmed", true))
                    .retrieve()
                    .body(Map.class);
            if (response == null) {
                throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Wallet reset returned an empty response");
            }
            return response;
        } catch (ResponseStatusException error) {
            throw error;
        } catch (RestClientException error) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Không thể reset Wallet. Kiểm tra wallet-service đang chạy với profile db-seed.", error);
        }
    }
}
