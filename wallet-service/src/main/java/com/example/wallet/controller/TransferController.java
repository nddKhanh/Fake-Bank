package com.example.wallet.controller;

import com.example.wallet.service.CrudTransferService.*;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
import java.util.List;

@RestController
@RequestMapping("/transfers")
public class TransferController {
    // Step 1.1 intentionally starts without the stage-2 Idempotency-Key requirement.
    @PostMapping public TransferResult create(@RequestBody CreateTransfer command) { throw new NotImplemented("CrudTransferService.create"); }
    @GetMapping public List<TransferResult> list() { throw new NotImplemented("CrudTransferService.list"); }
    @GetMapping("/{id}") public TransferResult get(@PathVariable UUID id) { throw new NotImplemented("CrudTransferService.get"); }
}
