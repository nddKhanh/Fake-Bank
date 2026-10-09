package com.example.wallet.api;

import org.springframework.web.bind.annotation.*;
import java.util.Map;
import java.util.UUID;
import static com.example.wallet.ops.OpsContracts.*;

@RestController
@RequestMapping("/api/ops")
public class OpsController {
    @GetMapping("/capabilities") public Map<String,Object> capabilities() {
        return Map.of("backendImplemented",false,"labEnabled",false,"dataSource","UNIMPLEMENTED");
    }
    @GetMapping("/overview") public Overview overview() { throw new NotImplemented("OpsQueryService.overview"); }
    @GetMapping("/transfers") public Page<Transfer> transfers(
        @RequestParam(required=false) String status, @RequestParam(required=false) String type,
        @RequestParam(required=false) String search, @RequestParam(required=false) String from,
        @RequestParam(required=false) String to, @RequestParam(defaultValue="0") int page,
        @RequestParam(defaultValue="50") int size) { throw new NotImplemented("OpsQueryService.transfers"); }
    @GetMapping("/transfers/{id}") public TransferDetail transfer(@PathVariable UUID id) { throw new NotImplemented("OpsQueryService.transfer"); }
    @GetMapping("/accounts") public Page<Account> accounts(
        @RequestParam(defaultValue="false") boolean mismatchedOnly, @RequestParam(required=false) String search,
        @RequestParam(defaultValue="0") int page, @RequestParam(defaultValue="50") int size) { throw new NotImplemented("OpsQueryService.accounts"); }
    @GetMapping("/accounts/{id}") public AccountDetail account(@PathVariable UUID id) { throw new NotImplemented("OpsQueryService.account"); }
    @GetMapping("/queues") public Queues queues() { throw new NotImplemented("OpsQueryService.queues"); }
    @GetMapping("/reconciliation") public Reconciliation reconciliation(@RequestParam(required=false) String status) { throw new NotImplemented("OpsQueryService.reconciliation"); }
    @GetMapping("/runs") public Page<ScenarioRun> runs(@RequestParam(defaultValue="0") int page,
        @RequestParam(defaultValue="50") int size) { throw new NotImplemented("OpsQueryService.runs"); }
    @GetMapping("/runs/{id}") public ScenarioRun run(@PathVariable UUID id) { throw new NotImplemented("OpsQueryService.run"); }
}
