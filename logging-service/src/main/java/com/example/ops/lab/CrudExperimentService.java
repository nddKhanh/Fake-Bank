package com.example.ops.lab;

import com.example.ops.ops.WalletResetClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Instant;
import java.util.UUID;

import static com.example.ops.ops.OpsContracts.*;

/** Runs small, destructive experiments against the intentionally naive Wallet V0 CRUD. */
@Service
public class CrudExperimentService {
    private static final String A = "00000000-0000-0000-0000-00000000000a";
    private static final String B = "00000000-0000-0000-0000-00000000000b";

    private final WalletResetClient reset;
    private final JdbcTemplate jdbc;
    private final RestClient wallet;

    public CrudExperimentService(WalletResetClient reset, JdbcTemplate jdbc,
                                 @Value("${wallet.url}") String walletUrl) {
        this.reset = reset;
        this.jdbc = jdbc;
        this.wallet = RestClient.builder().baseUrl(walletUrl).build();
    }

    public CrudExperimentResult run(String id) {
        RunTrace trace = new RunTrace();
        reset.reset();
        trace.record("POST", "/api/dev/reset", "confirmed=true", 200);
        CrudSnapshot before = snapshot();
        return switch (id.toUpperCase()) {
            case "V0-VALIDATION" -> validation(before, trace);
            case "V0-PARTIAL-WRITE" -> partialWrite(before, trace);
            case "V0-OVERDRAFT" -> overdraft(before, trace);
            case "V0-DUPLICATE" -> duplicate(before, trace);
            case "V0-CONCURRENCY" -> concurrency(before, trace);
            default -> throw new IllegalArgumentException("Unknown CRUD experiment: " + id);
        };
    }

    private CrudExperimentResult validation(CrudSnapshot before, RunTrace trace) {
        int status = transfer("0", trace);
        CrudSnapshot after = snapshot();
        boolean safe = status == 400 && before.equals(after);
        return result("V0-VALIDATION", "Amount bằng 0", safe ? "SAFE" : "UNEXPECTED",
                safe ? "Request bị từ chối và database không đổi. Validation cơ bản đang hoạt động."
                        : "Kết quả khác kỳ vọng: kiểm tra HTTP status và snapshot.",
                List.of(status), before, after, trace);
    }

    private CrudExperimentResult overdraft(CrudSnapshot before, RunTrace trace) {
        int status = transfer("120000", trace);
        CrudSnapshot after = snapshot();
        boolean reproduced = status == 201 && Long.parseLong(after.balances().get("user-A")) < 0;
        return result("V0-OVERDRAFT", "Chuyển quá số dư", reproduced ? "BUG_REPRODUCED" : "NOT_REPRODUCED",
                reproduced ? "Wallet V0 chấp nhận giao dịch và để user-A âm vì chưa kiểm tra đủ số dư."
                        : "Không thấy số dư âm; logic có thể đã được nâng cấp.",
                List.of(status), before, after, trace);
    }

    private CrudExperimentResult partialWrite(CrudSnapshot before, RunTrace trace) {
        int armStatus = armFault("AFTER_DEBIT", trace);
        int transferStatus = transfer("30000", trace);
        CrudSnapshot after = snapshot();
        boolean reproduced = armStatus == 200 && transferStatus >= 500
                && "70000".equals(after.balances().get("user-A"))
                && "50000".equals(after.balances().get("user-B"))
                && after.transferCount() == 0;
        return result("V0-PARTIAL-WRITE", "Lỗi sau khi trừ tiền", reproduced ? "BUG_REPRODUCED" : "NOT_REPRODUCED",
                reproduced ? "API lỗi sau bước debit: A đã mất 30.000, B chưa nhận và không có transfer để truy vết."
                        : "Không thấy partial write; logic có thể đã được bọc transaction.",
                List.of(transferStatus), before, after, trace);
    }

    private CrudExperimentResult duplicate(CrudSnapshot before, RunTrace trace) {
        List<Integer> statuses = List.of(transfer("10000", trace), transfer("10000", trace));
        CrudSnapshot after = snapshot();
        boolean reproduced = statuses.stream().allMatch(status -> status == 201) && after.transferCount() == 2;
        return result("V0-DUPLICATE", "Gửi lại cùng một yêu cầu", reproduced ? "BUG_REPRODUCED" : "NOT_REPRODUCED",
                reproduced ? "Cùng payload tạo hai transfer và trừ tiền hai lần vì V0 chưa có Idempotency-Key."
                        : "Yêu cầu trùng đã bị chặn hoặc kết quả khác kỳ vọng.",
                statuses, before, after, trace);
    }

    private CrudExperimentResult concurrency(CrudSnapshot before, RunTrace trace) {
        int requests = 20;
        CountDownLatch ready = new CountDownLatch(requests);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(requests);
        List<Future<Integer>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < requests; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    return transfer("10000", trace);
                }));
            }
            ready.await();
            start.countDown();
            List<Integer> statuses = new ArrayList<>();
            for (Future<Integer> future : futures) statuses.add(future.get());
            CrudSnapshot after = snapshot();
            long totalBefore = total(before);
            long totalAfter = total(after);
            long successes = statuses.stream().filter(status -> status == 201).count();
            boolean reproduced = successes > 10 || totalBefore != totalAfter
                    || Long.parseLong(after.balances().get("user-A")) < 0;
            String evidence = "Thành công " + successes + "/" + requests
                    + "; tổng tiền trước=" + totalBefore + ", sau=" + totalAfter + ". ";
            return result("V0-CONCURRENCY", "20 request đồng thời", reproduced ? "BUG_REPRODUCED" : "NOT_REPRODUCED",
                    evidence + (reproduced
                            ? "V0 không khóa tài khoản và không giới hạn theo số dư."
                            : "Lần chạy này chưa tái hiện lỗi; chạy lại vì race condition không luôn xuất hiện giống nhau."),
                    statuses, before, after, trace);
        } catch (Exception error) {
            throw new IllegalStateException("Concurrency experiment failed", error);
        } finally {
            start.countDown();
            pool.shutdownNow();
        }
    }

    private int transfer(String amount, RunTrace trace) {
        int sequence = trace.nextSequence();
        int status = wallet.post().uri("/transfers")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("fromAccountId", A, "toAccountId", B, "amount", amount))
                .exchange((request, response) -> response.getStatusCode().value());
        trace.record(sequence, "POST", "/transfers",
                "from=" + A + ", to=" + B + ", amount=" + amount, status);
        return status;
    }

    private int armFault(String point, RunTrace trace) {
        int sequence = trace.nextSequence();
        int status = wallet.post().uri("/api/dev/faults/next-transfer")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("point", point, "confirmed", true))
                .exchange((request, response) -> response.getStatusCode().value());
        trace.record(sequence, "POST", "/api/dev/faults/next-transfer", "point=" + point, status);
        return status;
    }

    private CrudSnapshot snapshot() {
        Map<String, String> balances = new LinkedHashMap<>();
        jdbc.query("SELECT owner_ref, balance FROM accounts ORDER BY owner_ref", rs -> {
            balances.put(rs.getString("owner_ref"), Long.toString(rs.getLong("balance")));
        });
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM transfers", Long.class);
        return new CrudSnapshot(Map.copyOf(balances), count == null ? 0 : count);
    }

    private static long total(CrudSnapshot snapshot) {
        return snapshot.balances().values().stream().mapToLong(Long::parseLong).sum();
    }

    private static CrudExperimentResult result(String id, String title, String verdict, String explanation,
                                               List<Integer> statuses, CrudSnapshot before, CrudSnapshot after,
                                               RunTrace trace) {
        return new CrudExperimentResult(UUID.randomUUID(), Instant.now(), "LIVE_BACKEND",
                id, title, verdict, explanation, trace.actions(), List.copyOf(statuses), before, after);
    }

    private static final class RunTrace {
        private final AtomicInteger sequence = new AtomicInteger();
        private final List<ExperimentAction> actions = Collections.synchronizedList(new ArrayList<>());

        private int nextSequence() {
            return sequence.incrementAndGet();
        }

        private void record(String method, String path, String request, int status) {
            record(nextSequence(), method, path, request, status);
        }

        private void record(int order, String method, String path, String request, int status) {
            actions.add(new ExperimentAction(order, method, path, request, status));
        }

        private List<ExperimentAction> actions() {
            synchronized (actions) {
                return actions.stream().sorted(Comparator.comparingInt(ExperimentAction::sequence)).toList();
            }
        }
    }
}
