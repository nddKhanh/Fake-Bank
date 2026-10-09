package com.example.wallet.ops;

import java.time.Instant;
import java.util.UUID;
import static com.example.wallet.ops.OpsContracts.*;

/** TODO: implement using a separate SELECT-only ops_reader datasource; never use the wallet writer. */
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
}
