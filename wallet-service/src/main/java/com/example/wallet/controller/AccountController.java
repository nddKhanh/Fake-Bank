package com.example.wallet.controller;

import com.example.wallet.dto.request.AccountResult;
import com.example.wallet.dto.request.CreateAccount;
import com.example.wallet.dto.request.UpdateAccount;
import com.example.wallet.exception.NotImplemented;
import com.example.wallet.common.response.ApiResponse;
import com.example.wallet.common.response.ListResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.*;

/** Basic account CRUD; enable profile db to connect to PostgreSQL. */
@RestController
@RequestMapping("/accounts")
public class AccountController {
    private final org.springframework.beans.factory.ObjectProvider<com.example.wallet.service.AccountService> services;
    public AccountController(org.springframework.beans.factory.ObjectProvider<com.example.wallet.service.AccountService> services) { this.services = services; }
    private com.example.wallet.service.AccountService service() {
        var service = services.getIfAvailable();
        if (service == null) throw new NotImplemented("Enable profile db to use account CRUD");
        return service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ApiResponse<AccountResult> create(@RequestBody CreateAccount command) {
        return ApiResponse.created("Account created", service().create(command));
    }

    @GetMapping
    public ApiResponse<ListResponse<AccountResult>> list() {
        return ApiResponse.success("Accounts retrieved", ListResponse.of(service().list()));

    }
    @GetMapping("/{id}")
    public ApiResponse<AccountResult> get(@PathVariable UUID id) {
        return ApiResponse.success("Account retrieved", service().get(id));
    }

    @PutMapping("/{id}")
    public ApiResponse<AccountResult> update(
            @PathVariable UUID id,
            @RequestBody UpdateAccount command
    ) {
        return ApiResponse.success("Account updated", service().update(id, command));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable UUID id) {
        service().delete(id); return ApiResponse.success("Account deleted");
    }
}
