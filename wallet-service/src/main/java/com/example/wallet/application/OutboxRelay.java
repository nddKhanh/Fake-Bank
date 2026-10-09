package com.example.wallet.application;

/** TODO: publish committed outbox events; stable event IDs, retry, idempotent consumers and DLT. */
public interface OutboxRelay {
    void publishBatch();
}
