package com.example.wallet.controller;

import com.example.wallet.common.response.ApiResponse;
import com.example.wallet.service.LocalTransferFaults;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("db-seed")
@RequestMapping("/api/dev/faults")
public class LocalTransferFaultController {
    private final LocalTransferFaults faults;

    public LocalTransferFaultController(LocalTransferFaults faults) {
        this.faults = faults;
    }

    @PostMapping("/next-transfer")
    public ApiResponse<Void> arm(@RequestBody FaultRequest request, HttpServletRequest httpRequest) {
        if (!isLoopback(httpRequest.getRemoteAddr())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Fault injection is restricted to localhost");
        }
        if (request == null || !request.confirmed()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "confirmed=true is required");
        }
        try {
            faults.arm(LocalTransferFaults.Point.valueOf(request.point()));
        } catch (IllegalArgumentException error) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported fault point");
        }
        return ApiResponse.success("The next transfer will fail once at " + request.point());
    }

    private static boolean isLoopback(String address) {
        return "127.0.0.1".equals(address) || "0:0:0:0:0:0:0:1".equals(address) || "::1".equals(address);
    }

    public record FaultRequest(String point, boolean confirmed) {}
}
