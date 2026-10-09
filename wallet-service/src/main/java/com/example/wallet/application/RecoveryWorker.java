package com.example.wallet.application;

/** TODO: claim stale transfers, inquire UNKNOWN, bounded retry, no DB transaction around bank HTTP. */
public interface RecoveryWorker {
    void recoverBatch();
}
