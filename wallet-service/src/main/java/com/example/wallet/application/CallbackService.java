package com.example.wallet.application;

/** TODO: verify signature, persist inbox, deduplicate bank_code/event_id and guard terminal states. */
public interface CallbackService {
    void receive(String rawPayload, String signature);
    void processBatch();
}
