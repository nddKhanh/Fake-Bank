package com.example.wallet.dto.request;

public record UpdateAccount(
        String ownerRef,
        String currency,
        String balance
) {}

