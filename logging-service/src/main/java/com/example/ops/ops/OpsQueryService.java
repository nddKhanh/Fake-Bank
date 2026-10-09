package com.example.ops.ops;

import java.time.Instant;
import java.util.UUID;
import static com.example.ops.ops.OpsContracts.*;

/** Read-only projection used by the Ops UI. */
public interface OpsQueryService {
    Overview overview();
    Page<Transfer> transfers(String status, String type, String search, Instant from, Instant to, int page, int size);
    TransferDetail transfer(UUID id);
    Page<Account> accounts(boolean mismatchedOnly, String search, int page, int size);
    AccountDetail account(UUID id);
    Queues queues();
    Reconciliation reconciliation(String status);
    Page<ScenarioRun> runs(int page, int size);
    ScenarioRun run(UUID id);
    DatabaseSchema databaseSchema();
}

