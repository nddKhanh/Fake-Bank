package com.example.wallet.application;

import java.util.UUID;

/** TODO: transaction, ordered row locks, persisted idempotency, ledger, limits and state machine. */
public interface TransferService {
    record Command(String type, UUID fromAccountId, UUID toAccountId, String amount,
                   String currency, String bankAccountNumber, String source, String sourceId) {}
    record Result(UUID transferId, String status, String failureCode) {}
    Result create(String idempotencyKey, Command command);
    Result inquiry(UUID transferId);
}
