package com.example.ops.controller;

import com.example.ops.ops.WalletResetClient;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

import static com.example.ops.ops.OpsContracts.ResetRequest;

@RestController
@RequestMapping("/api/ops")
public class LocalResetController {
    private final WalletResetClient wallet;

    public LocalResetController(WalletResetClient wallet) {
        this.wallet = wallet;
    }

    @PostMapping("/reset")
    public Map<String, Object> reset(@RequestBody ResetRequest request, HttpServletRequest httpRequest) {
        if (!isLoopback(httpRequest.getRemoteAddr())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Reset is restricted to localhost");
        }
        if (request == null || !request.confirmed()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "confirmed=true is required");
        }
        return wallet.reset();
    }

    private static boolean isLoopback(String address) {
        return "127.0.0.1".equals(address) || "0:0:0:0:0:0:0:1".equals(address) || "::1".equals(address);
    }
}
