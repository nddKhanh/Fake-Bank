package com.example.wallet.controller;

import com.example.wallet.common.response.ApiResponse;
import com.example.wallet.service.LocalSeedResetService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("db-seed")
@RequestMapping("/api/dev")
public class LocalSeedResetController {
    private final LocalSeedResetService resetService;

    public LocalSeedResetController(LocalSeedResetService resetService) {
        this.resetService = resetService;
    }

    @PostMapping("/reset")
    public ApiResponse<LocalSeedResetService.ResetResult> reset(
            @RequestBody ResetRequest request,
            HttpServletRequest httpRequest
    ) {
        if (!httpRequest.getRemoteAddr().equals("127.0.0.1")
                && !httpRequest.getRemoteAddr().equals("0:0:0:0:0:0:0:1")
                && !httpRequest.getRemoteAddr().equals("::1")) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Reset is restricted to localhost");
        }
        if (request == null || !request.confirmed()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "confirmed=true is required");
        }
        return ApiResponse.success("Wallet restored to the initial V0 seed", resetService.reset());
    }

    public record ResetRequest(boolean confirmed) {}
}
