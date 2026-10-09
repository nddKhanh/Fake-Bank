package com.example.wallet.application;

import java.util.UUID;

/** TODO: a single posting entry point; ordered locks, account_seq, balanced append-only postings. */
public interface LedgerService {
    record Posting(UUID transferId, String type, UUID debitAccountId, UUID creditAccountId,
                   String amount, UUID originalTransactionId) {}
    UUID post(Posting posting);
}
