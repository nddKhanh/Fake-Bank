package com.example.wallet.controller;

import com.example.wallet.exception.NotImplemented;
import com.example.wallet.common.response.ApiResponse;
import com.example.wallet.common.response.ListResponse;
import org.springframework.http.HttpStatus;
import com.example.wallet.service.TransferService.*;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

@RestController
@RequestMapping("/transfers")
public class TransferController {
    private final org.springframework.beans.factory.ObjectProvider<com.example.wallet.service.TransferService> services;
    public TransferController(org.springframework.beans.factory.ObjectProvider<com.example.wallet.service.TransferService> services) { this.services = services; }
    private com.example.wallet.service.TransferService service() {
        var service = services.getIfAvailable();
        if (service == null) throw new NotImplemented("Enable profile db to use transfers");
        return service;
    }
    // Step 1.1 intentionally starts without the stage-2 Idempotency-Key requirement.
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ApiResponse<TransferResult> create(@RequestBody CreateTransfer command) {
        return ApiResponse.created("Transfer created", service().create(command));
    }

    @GetMapping
    public ApiResponse<ListResponse<TransferResult>> list() {
        return ApiResponse.success("Transfers retrieved", ListResponse.of(service().list()));
    }

    @GetMapping("/{id}")
    public ApiResponse<TransferResult> get(@PathVariable UUID id) {
        return ApiResponse.success("Transfer retrieved", service().get(id));
    }
}
