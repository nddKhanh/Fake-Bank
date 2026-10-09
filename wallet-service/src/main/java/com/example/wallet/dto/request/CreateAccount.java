package com.example.wallet.dto.request;

public record CreateAccount(
        String ownerRef,
        String currency,
        String balance
) {}

