package com.example.wallet.controller;

import org.springframework.web.bind.annotation.*;
import java.util.*;
import static com.example.wallet.service.AccountService.*;

/** TODO: wire your AccountService implementation. Every operation is currently a 501 stub. */
@RestController
@RequestMapping("/accounts")
public class AccountController {
    @PostMapping public AccountResult create(@RequestBody CreateAccount command) { throw new NotImplemented("AccountService.create"); }
    @GetMapping public List<AccountResult> list() { throw new NotImplemented("AccountService.list"); }
    @GetMapping("/{id}") public AccountResult get(@PathVariable UUID id) { throw new NotImplemented("AccountService.get"); }
    @PutMapping("/{id}") public AccountResult update(@PathVariable UUID id, @RequestBody UpdateAccount command) { throw new NotImplemented("AccountService.update"); }
    @DeleteMapping("/{id}") public void delete(@PathVariable UUID id) { throw new NotImplemented("AccountService.delete"); }
}
