package com.example.wallet.application;

import java.time.Instant;
import java.util.*;

/** TODO: implement the initial account CRUD use cases. */
public interface AccountService {
    record CreateAccount(String ownerRef, String currency, String balance) {}
    record UpdateAccount(String ownerRef, String currency, String balance) {}
    record AccountResult(UUID id, String ownerRef, String currency, String balance, Instant createdAt) {}
    AccountResult create(CreateAccount command);
    AccountResult get(UUID id);
    List<AccountResult> list();
    AccountResult update(UUID id, UpdateAccount command);
    void delete(UUID id);
}
