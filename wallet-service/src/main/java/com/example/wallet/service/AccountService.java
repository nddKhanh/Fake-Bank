package com.example.wallet.service;

import com.example.wallet.dto.request.AccountResult;
import com.example.wallet.dto.request.CreateAccount;
import com.example.wallet.dto.request.UpdateAccount;

import java.time.Instant;
import java.util.*;

/** Initial account CRUD use cases, implemented by AccountServiceImpl. */
public interface AccountService {
    AccountResult create(CreateAccount command);
    AccountResult get(UUID id);
    List<AccountResult> list();
    AccountResult update(UUID id, UpdateAccount command);
    void delete(UUID id);
}
