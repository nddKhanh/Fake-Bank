package com.example.wallet.service;

import java.time.Instant;
import java.util.*;

/**
 * Entry point for chapter 2, step 1.1, implemented by TransferServiceImpl.
 * This initial scaffold has no idempotency key requirement.
 */
public interface TransferService {
    record CreateTransfer(UUID fromAccountId, UUID toAccountId, String amount) {}
    record TransferResult(UUID id, UUID fromAccountId, UUID toAccountId, String amount, Instant createdAt) {}
    TransferResult create(CreateTransfer command);
    TransferResult get(UUID id);
    List<TransferResult> list();
}
