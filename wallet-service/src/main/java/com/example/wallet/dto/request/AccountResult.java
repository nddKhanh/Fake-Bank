package com.example.wallet.dto.request;

import java.time.Instant;
import java.util.UUID;

public record AccountResult(
        UUID id,
        String ownerRef,
        String currency,
        String balance,
        Instant createdAt
) {}

