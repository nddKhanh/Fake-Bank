package com.example.ops.ops;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.example.ops.ops.OpsContracts.*;

/**
 * Read-only Ops projection for the current Wallet V0/V1 schema.
 * Advanced fields are left null/empty rather than being fabricated.
 */
@Service
@Transactional(readOnly = true)
public class JdbcOpsQueryService implements OpsQueryService {
    private static final String TRANSFER_SELECT = """
            SELECT t.id, t.from_account_id, t.to_account_id, t.amount, t.created_at,
                   TRIM(a.currency) AS currency
            FROM transfers t
            JOIN accounts a ON a.id = t.from_account_id
            """;

    private final JdbcTemplate jdbc;

    public JdbcOpsQueryService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Overview overview() {
        long negativeAccounts = count("SELECT COUNT(*) FROM accounts WHERE balance < 0");
        long invalidAmounts = count("SELECT COUNT(*) FROM transfers WHERE amount <= 0");
        long currencyMismatches = count("""
                SELECT COUNT(*) FROM transfers t
                JOIN accounts source ON source.id = t.from_account_id
                JOIN accounts destination ON destination.id = t.to_account_id
                WHERE source.currency <> destination.currency
                """);
        long brokenReferences = count("""
                SELECT COUNT(*) FROM transfers t
                LEFT JOIN accounts source ON source.id = t.from_account_id
                LEFT JOIN accounts destination ON destination.id = t.to_account_id
                WHERE source.id IS NULL OR destination.id IS NULL
                """);
        long accountCount = count("SELECT COUNT(*) FROM accounts");
        long transferCount = count("SELECT COUNT(*) FROM transfers");

        List<Check> checks = List.of(
                new Check(1, "Không tài khoản nào có số dư âm", negativeAccounts, "#accounts"),
                new Check(2, "Mọi giao dịch có số tiền dương", invalidAmounts, "#transfers"),
                new Check(3, "Hai tài khoản giao dịch cùng loại tiền", currencyMismatches, "#transfers"),
                new Check(4, "Mọi giao dịch tham chiếu tài khoản tồn tại", brokenReferences, "#transfers")
        );
        List<Metric> metrics = List.of(
                new Metric("accounts", "Tổng tài khoản", accountCount, "OK", "#accounts"),
                new Metric("transfers", "Tổng giao dịch", transferCount, "OK", "#transfers"),
                new Metric("open", "Giao dịch đang dở", 0, "OK", "#queues"),
                new Metric("unknown", "Giao dịch UNKNOWN", 0, "OK", "#transfers?status=UNKNOWN"),
                new Metric("review", "Cần người xem", 0, "OK", "#transfers?status=PENDING_REVIEW"),
                new Metric("issues", "Sai lệch đối soát", 0, "OK", "#reconciliation")
        );
        List<Bucket> buckets = jdbc.query("""
                SELECT date_trunc('minute', created_at) AS minute, COUNT(*) AS total
                FROM transfers
                GROUP BY date_trunc('minute', created_at)
                ORDER BY minute
                """, (rs, rowNum) -> new Bucket(instant(rs, "minute"), "COMPLETED", rs.getLong("total")));
        return new Overview(checks, metrics, buckets, "N/A ở schema V0", "N/A ở schema V0", Instant.now());
    }

    @Override
    public Page<Transfer> transfers(String status, String type, String search, Instant from,
                                    Instant to, int page, int size) {
        if (notBlank(status) && !"COMPLETED".equalsIgnoreCase(status)
                || notBlank(type) && !"INTERNAL".equalsIgnoreCase(type)) {
            return new Page<>(List.of(), 0, page, size);
        }

        List<String> predicates = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        if (notBlank(search)) {
            predicates.add("LOWER(CAST(t.id AS VARCHAR)) LIKE ?");
            args.add("%" + search.trim().toLowerCase() + "%");
        }
        if (from != null) {
            predicates.add("t.created_at >= ?");
            args.add(Timestamp.from(from));
        }
        if (to != null) {
            predicates.add("t.created_at <= ?");
            args.add(Timestamp.from(to));
        }
        String where = predicates.isEmpty() ? "" : " WHERE " + String.join(" AND ", predicates);
        long total = queryCount("SELECT COUNT(*) FROM transfers t" + where, args);

        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(size);
        pageArgs.add((long) page * size);
        List<Transfer> items = jdbc.query(
                TRANSFER_SELECT + where + " ORDER BY t.created_at DESC, t.id DESC LIMIT ? OFFSET ?",
                this::mapTransfer,
                pageArgs.toArray()
        );
        return new Page<>(items, total, page, size);
    }

    @Override
    public TransferDetail transfer(UUID id) {
        List<Transfer> matches = jdbc.query(
                TRANSFER_SELECT + " WHERE t.id = ?",
                this::mapTransfer,
                id
        );
        if (matches.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Transfer not found");
        }
        Transfer transfer = matches.get(0);
        String sourceName = accountLabel(transfer.fromAccountId());
        String destinationName = accountLabel(transfer.toAccountId());
        List<Event> timeline = List.of(new Event(
                transfer.createdAt(),
                "WALLET_V0",
                "COMPLETED",
                "Giao dịch được lưu sau khi cập nhật số dư nguồn và đích"
        ));
        List<Posting> postings = List.of(
                new Posting(stableId(id, "DEBIT"), "TRANSFER_V0", sourceName,
                        "DEBIT", transfer.amount(), null),
                new Posting(stableId(id, "CREDIT"), "TRANSFER_V0", destinationName,
                        "CREDIT", transfer.amount(), null)
        );
        return new TransferDetail(transfer, timeline, postings, List.of());
    }

    @Override
    public Page<Account> accounts(boolean mismatchedOnly, String search, int page, int size) {
        // A mismatch cannot be calculated until the ledger tables exist.
        if (mismatchedOnly) {
            return new Page<>(List.of(), 0, page, size);
        }
        List<Object> args = new ArrayList<>();
        String where = "";
        if (notBlank(search)) {
            where = " WHERE LOWER(a.owner_ref) LIKE ? OR LOWER(CAST(a.id AS VARCHAR)) LIKE ?"
                    + " OR LOWER('ACC-' || RIGHT(REPLACE(CAST(a.id AS VARCHAR), '-', ''), 8)) LIKE ?";
            String value = "%" + search.trim().toLowerCase() + "%";
            args.add(value);
            args.add(value);
            args.add(value);
        }
        long total = queryCount("SELECT COUNT(*) FROM accounts a" + where, args);
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(size);
        pageArgs.add((long) page * size);
        List<Account> items = jdbc.query("""
                SELECT a.id, a.owner_ref, a.balance,
                       (SELECT COUNT(*) FROM transfers t
                        WHERE t.from_account_id = a.id OR t.to_account_id = a.id) AS transfer_count
                FROM accounts a
                """ + where + " ORDER BY a.created_at, a.id LIMIT ? OFFSET ?",
                this::mapAccount,
                pageArgs.toArray()
        );
        return new Page<>(items, total, page, size);
    }

    @Override
    public AccountDetail account(UUID id) {
        List<Account> matches = jdbc.query("""
                SELECT a.id, a.owner_ref, a.balance,
                       (SELECT COUNT(*) FROM transfers t
                        WHERE t.from_account_id = a.id OR t.to_account_id = a.id) AS transfer_count
                FROM accounts a WHERE a.id = ?
                """, this::mapAccount, id);
        if (matches.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found");
        }
        // V0 has no append-only ledger, so a trustworthy statement cannot be reconstructed.
        return new AccountDetail(matches.get(0), List.of());
    }

    @Override
    public Queues queues() {
        return new Queues(List.of(), List.of(), List.of(), List.of(), "NOT_AVAILABLE_IN_WALLET_V0");
    }

    @Override
    public Reconciliation reconciliation(String status) {
        return new Reconciliation(List.of(), List.of());
    }

    @Override
    public Page<ScenarioRun> runs(int page, int size) {
        return new Page<>(List.of(), 0, page, size);
    }

    @Override
    public ScenarioRun run(UUID id) {
        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Scenario run storage is not available");
    }

    @Override
    public DatabaseSchema databaseSchema() {
        Map<String, Object> identity = jdbc.queryForMap(
                "SELECT current_database() AS database_name, current_schema() AS schema_name, version() AS version");
        Map<String, TableBuilder> tables = new LinkedHashMap<>();
        jdbc.query("""
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                ORDER BY table_name
                """, rs -> {
            tables.put(rs.getString("table_name"),
                    new TableBuilder(rs.getString("table_name"), rs.getString("table_type")));
        });
        jdbc.query("""
                SELECT table_name, column_name, data_type, udt_name, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                ORDER BY table_name, ordinal_position
                """, rs -> {
            TableBuilder table = tables.get(rs.getString("table_name"));
            if (table != null) {
                String dataType = "USER-DEFINED".equals(rs.getString("data_type"))
                        ? rs.getString("udt_name") : rs.getString("data_type");
                table.columns.add(new DatabaseColumn(
                        rs.getString("column_name"), dataType,
                        "YES".equals(rs.getString("is_nullable")), rs.getString("column_default")));
            }
        });
        jdbc.query("""
                SELECT tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name,
                       ccu.table_name AS referenced_table, ccu.column_name AS referenced_column
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_catalog = kcu.constraint_catalog
                 AND tc.constraint_schema = kcu.constraint_schema
                 AND tc.constraint_name = kcu.constraint_name
                LEFT JOIN information_schema.constraint_column_usage ccu
                  ON tc.constraint_catalog = ccu.constraint_catalog
                 AND tc.constraint_schema = ccu.constraint_schema
                 AND tc.constraint_name = ccu.constraint_name
                WHERE tc.table_schema = current_schema()
                ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
                """, rs -> {
            TableBuilder table = tables.get(rs.getString("table_name"));
            if (table != null) {
                String type = rs.getString("constraint_type");
                table.constraints.add(new DatabaseConstraint(
                        rs.getString("constraint_name"), type, rs.getString("column_name"),
                        "FOREIGN KEY".equals(type) ? rs.getString("referenced_table") : null,
                        "FOREIGN KEY".equals(type) ? rs.getString("referenced_column") : null));
            }
        });
        return new DatabaseSchema(
                String.valueOf(identity.get("database_name")),
                String.valueOf(identity.get("schema_name")),
                String.valueOf(identity.get("version")),
                Instant.now(),
                tables.values().stream().map(TableBuilder::build).toList()
        );
    }

    private Transfer mapTransfer(ResultSet rs, int rowNum) throws SQLException {
        Instant createdAt = instant(rs, "created_at");
        return new Transfer(
                rs.getObject("id", UUID.class),
                "INTERNAL",
                "COMPLETED",
                Long.toString(rs.getLong("amount")),
                rs.getString("currency"),
                rs.getObject("from_account_id", UUID.class),
                rs.getObject("to_account_id", UUID.class),
                null,
                "WALLET_V0",
                null,
                null,
                null,
                createdAt,
                createdAt
        );
    }

    private Account mapAccount(ResultSet rs, int rowNum) throws SQLException {
        UUID id = rs.getObject("id", UUID.class);
        String compactId = id.toString().replace("-", "").toUpperCase();
        return new Account(
                id,
                "ACC-" + compactId.substring(compactId.length() - 8),
                rs.getString("owner_ref"),
                "USER",
                Long.toString(rs.getLong("balance")),
                null,
                null,
                rs.getLong("transfer_count")
        );
    }

    private long count(String sql) {
        Long value = jdbc.queryForObject(sql, Long.class);
        return value == null ? 0 : value;
    }

    private long queryCount(String sql, List<Object> args) {
        Long value = jdbc.queryForObject(sql, Long.class, args.toArray());
        return value == null ? 0 : value;
    }

    private String accountLabel(UUID id) {
        List<String> owners = jdbc.query(
                "SELECT owner_ref FROM accounts WHERE id = ?",
                (rs, rowNum) -> rs.getString("owner_ref"),
                id
        );
        return owners.isEmpty() ? id.toString() : owners.get(0);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private static UUID stableId(UUID transferId, String leg) {
        return UUID.nameUUIDFromBytes((transferId + ":" + leg).getBytes(StandardCharsets.UTF_8));
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static final class TableBuilder {
        private final String name;
        private final String type;
        private final List<DatabaseColumn> columns = new ArrayList<>();
        private final List<DatabaseConstraint> constraints = new ArrayList<>();

        private TableBuilder(String name, String type) {
            this.name = name;
            this.type = type;
        }

        private DatabaseTable build() {
            return new DatabaseTable(name, type, List.copyOf(columns), List.copyOf(constraints));
        }
    }
}
