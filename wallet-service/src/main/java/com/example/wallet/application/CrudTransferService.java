package com.example.wallet.application;

import java.time.Instant;
import java.util.*;

/**
 * Entry point for chapter 2, step 1.1. TODO: write the initial implementation yourself.
 * Keep the advanced TransferService interface for later stages; this one has no idempotency key.
 */
public interface CrudTransferService {
    record CreateTransfer(UUID fromAccountId, UUID toAccountId, String amount) {}
    record TransferResult(UUID id, UUID fromAccountId, UUID toAccountId, String amount, Instant createdAt) {}
    TransferResult create(CreateTransfer command);
    TransferResult get(UUID id);
    List<TransferResult> list();
}
