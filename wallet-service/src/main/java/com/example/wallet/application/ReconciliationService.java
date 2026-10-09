package com.example.wallet.application;

/** TODO: cache vs ledger, bank report vs transfers; corrections use ADJUSTMENT postings with audit links. */
public interface ReconciliationService {
    void reconcileInternal();
    void reconcileBankReport(String report);
}
