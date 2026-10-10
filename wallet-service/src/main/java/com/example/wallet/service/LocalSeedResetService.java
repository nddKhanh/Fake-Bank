package com.example.wallet.service;

import com.example.wallet.domain.Account;
import com.example.wallet.repository.AccountRepository;
import com.example.wallet.repository.TransferRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Destructive local-only reset, available only while the db-seed profile is active. */
@Service
@Profile("db-seed")
public class LocalSeedResetService {
    private static final UUID ACCOUNT_A = UUID.fromString("00000000-0000-0000-0000-00000000000a");
    private static final UUID ACCOUNT_B = UUID.fromString("00000000-0000-0000-0000-00000000000b");
    private static final UUID ACCOUNT_C = UUID.fromString("00000000-0000-0000-0000-00000000000c");

    private final AccountRepository accounts;
    private final TransferRepository transfers;
    private final ObjectProvider<LocalTransferFaults> localFaults;

    @Autowired
    public LocalSeedResetService(AccountRepository accounts, TransferRepository transfers,
                                 ObjectProvider<LocalTransferFaults> localFaults) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.localFaults = localFaults;
    }

    public LocalSeedResetService(AccountRepository accounts, TransferRepository transfers) {
        this(accounts, transfers, null);
    }

    @Transactional
    public ResetResult reset() {
        if (localFaults != null) localFaults.ifAvailable(LocalTransferFaults::clear);
        long removedTransfers = transfers.count();
        long removedAccounts = accounts.count();

        transfers.deleteAllInBatch();
        transfers.flush();
        accounts.deleteAllInBatch();
        accounts.flush();

        Instant createdAt = Instant.now();
        accounts.saveAllAndFlush(List.of(
                account(ACCOUNT_A, "user-A", 100_000, createdAt),
                account(ACCOUNT_B, "user-B", 50_000, createdAt),
                account(ACCOUNT_C, "user-C", 0, createdAt)
        ));
        return new ResetResult(removedTransfers, removedAccounts, 3);
    }

    private static Account account(UUID id, String ownerRef, long balance, Instant createdAt) {
        return Account.builder()
                .id(id)
                .ownerRef(ownerRef)
                .currency("VND")
                .balance(balance)
                .createdAt(createdAt)
                .build();
    }

    public record ResetResult(long removedTransfers, long removedAccounts, int seededAccounts) {}
}
