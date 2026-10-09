package com.example.ops.ops;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** API contracts for screens S1–S7. Money is a string to preserve BIGINT precision in JavaScript. */
public final class OpsContracts {
    private OpsContracts() {}
    public record Check(int sortOrder, String checkName, long violations, String drilldown) {}
    public record Metric(String key, String label, long value, String level, String drilldown) {}
    public record Bucket(Instant minute, String status, long count) {}
    public record Overview(List<Check> checks, List<Metric> metrics, List<Bucket> buckets,
                           String p50, String p95, Instant observedAt) {}
    public record Transfer(UUID id, String type, String status, String amount, String currency,
                           UUID fromAccountId, UUID toAccountId, String idempotencyKey,
                           String source, String sourceId, String bankRequestId,
                           String failureCode, Instant createdAt, Instant updatedAt) {}
    public record Page<T>(List<T> items, long total, int page, int size) {}
    public record Event(Instant at, String source, String what, String detail) {}
    public record Posting(UUID id, String type, String accountNumber, String direction,
                          String amount, String balanceAfter) {}
    public record Attempt(int attemptNo, String operation, String bankRequestId, String bankReference,
                          String status, Integer httpStatus, String errorCode, Instant sentAt, Instant respondedAt) {}
    public record TransferDetail(Transfer transfer, List<Event> timeline, List<Posting> postings, List<Attempt> attempts) {}
    public record Account(UUID id, String accountNumber, String ownerRef, String type,
                          String cachedBalance, String ledgerBalance, String diff, long entryCount) {}
    public record Statement(long accountSeq, Instant createdAt, String ledgerType, String direction,
                            String amount, String balanceAfter, String recomputedBalance, UUID transferId) {}
    public record AccountDetail(Account account, List<Statement> statement) {}
    public record QueueItem(String id, UUID transferId, String eventType, String status, int attempts,
                            Instant createdAt, Boolean signatureValid) {}
    public record ChaosRule(String name, String mode, boolean enabled, String probability, Map<String,Object> params) {}
    public record Queues(List<Transfer> openTransfers, List<QueueItem> outbox, List<QueueItem> callbacks,
                         List<ChaosRule> chaosRules, String transport) {}
    public record Issue(String id, String issueType, String status, UUID transferId, UUID accountId,
                        String expectedAmount, String actualAmount, String resolutionNote,
                        String adjustmentId, Instant createdAt) {}
    public record ReconciliationRun(String id, Instant createdAt, long issueCount) {}
    public record Reconciliation(List<Issue> issues, List<ReconciliationRun> runs) {}
    public record Assertion(String label, boolean passed, String actual, String expected, UUID transferId) {}
    public record Snapshot(Map<String,String> balances, Map<String,Long> transfers) {}
    public record ScenarioRun(UUID id, String scenarioId, long seed, String status, Instant startedAt,
                              Instant finishedAt, String phase, boolean converged, int stablePolls,
                              Snapshot before, Snapshot after, List<Check> checks,
                              List<Assertion> assertions, List<UUID> transferIds, String error) {}
    public record RunRequest(long seed) {}
    public record ToggleRequest(boolean enabled) {}
    public record ResetRequest(boolean confirmed) {}
    public record DatabaseSchema(String databaseName, String schemaName, String databaseVersion,
                                 Instant observedAt, List<DatabaseTable> tables) {}
    public record DatabaseTable(String name, String type, List<DatabaseColumn> columns,
                                List<DatabaseConstraint> constraints) {}
    public record DatabaseColumn(String name, String dataType, boolean nullable, String defaultValue) {}
    public record DatabaseConstraint(String name, String type, String columnName,
                                     String referencedTable, String referencedColumn) {}
    public record ExperimentRequest(boolean confirmed) {}
    public record CrudSnapshot(Map<String,String> balances, long transferCount) {}
    public record CrudExperimentResult(String id, String title, String verdict, String explanation,
                                       List<Integer> httpStatuses, CrudSnapshot before, CrudSnapshot after) {}
}

