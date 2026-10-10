package com.example.ops.controller;

import com.example.ops.ops.OpsQueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static com.example.ops.ops.OpsContracts.*;

@RestController
@RequestMapping("/api/ops")
public class OpsController {
    private final OpsQueryService queries;

    public OpsController(OpsQueryService queries) {
        this.queries = queries;
    }

    @GetMapping("/capabilities") public Map<String,Object> capabilities() {
        return Map.of(
                "backendImplemented", true,
                "labEnabled", false,
                "resetEnabled", true,
                "crudExperimentsEnabled", true,
                "crudExperimentsMode", "LIVE_BACKEND",
                "dataSource", "WALLET_V0_READ_ONLY",
                "limitations", List.of("ledger", "outbox", "callbacks", "reconciliation", "scenario-runs")
        );
    }
    @GetMapping("/overview") public Overview overview() { return queries.overview(); }
    @GetMapping("/transfers") public Page<Transfer> transfers(
        @RequestParam(required=false) String status, @RequestParam(required=false) String type,
        @RequestParam(required=false) String search,
        @RequestParam(required=false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
        @RequestParam(required=false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
        @RequestParam(defaultValue="0") int page,
        @RequestParam(defaultValue="50") int size) {
        validatePage(page, size);
        if (from != null && to != null && from.isAfter(to)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "from must be before to");
        }
        return queries.transfers(status, type, search, from, to, page, size);
    }
    @GetMapping("/transfers/{id}") public TransferDetail transfer(@PathVariable UUID id) { return queries.transfer(id); }
    @GetMapping("/accounts") public Page<Account> accounts(
        @RequestParam(defaultValue="false") boolean mismatchedOnly, @RequestParam(required=false) String search,
        @RequestParam(defaultValue="0") int page, @RequestParam(defaultValue="50") int size) {
        validatePage(page, size);
        return queries.accounts(mismatchedOnly, search, page, size);
    }
    @GetMapping("/accounts/{id}") public AccountDetail account(@PathVariable UUID id) { return queries.account(id); }
    @GetMapping("/queues") public Queues queues() { return queries.queues(); }
    @GetMapping("/reconciliation") public Reconciliation reconciliation(@RequestParam(required=false) String status) { return queries.reconciliation(status); }
    @GetMapping("/runs") public Page<ScenarioRun> runs(@RequestParam(defaultValue="0") int page,
        @RequestParam(defaultValue="50") int size) {
        validatePage(page, size);
        return queries.runs(page, size);
    }
    @GetMapping("/runs/{id}") public ScenarioRun run(@PathVariable UUID id) { return queries.run(id); }
    @GetMapping("/schema") public DatabaseSchema databaseSchema() { return queries.databaseSchema(); }

    private static void validatePage(int page, int size) {
        if (page < 0 || size < 1 || size > 200) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "page must be >= 0 and size must be 1-200");
        }
    }
}

