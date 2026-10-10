package com.example.wallet.service.impl;

import com.example.wallet.domain.Account;
import com.example.wallet.domain.Transfer;
import com.example.wallet.repository.AccountRepository;
import com.example.wallet.repository.TransferRepository;
import com.example.wallet.service.TransferService;
import com.example.wallet.service.LocalTransferFaults;
import com.example.wallet.common.utils.AccountUtils;
import com.example.wallet.common.utils.MoneyUtils;
import com.example.wallet.common.utils.TransferUtils;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Chuyển tiền CRUD ở bậc 1.1: ba lần ghi độc lập.
 * Chưa có transaction bao quanh cả luồng và chưa khóa tài khoản.
 */
@Service
@Profile("db")
public class TransferServiceImpl implements TransferService {

    private final AccountRepository accounts;
    private final TransferRepository transfers;
    private final ObjectProvider<LocalTransferFaults> localFaults;

    @Autowired
    public TransferServiceImpl(
            AccountRepository accounts,
            TransferRepository transfers,
            ObjectProvider<LocalTransferFaults> localFaults
    ) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.localFaults = localFaults;
    }

    /** Convenience constructor kept for focused unit tests outside a Spring context. */
    public TransferServiceImpl(AccountRepository accounts, TransferRepository transfers) {
        this(accounts, transfers, null);
    }

    @Override
    public TransferResult create(CreateTransfer command) {
        // Hai tài khoản phải được cung cấp và không trùng nhau.
        if (command.fromAccountId() == null
                || command.toAccountId() == null
                || command.fromAccountId().equals(command.toAccountId())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Two different account IDs are required"
            );
        }

        long amount = MoneyUtils.parseMoney(command.amount());

        if (amount <= 0) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "amount must be positive"
            );
        }

        // Đọc số dư hiện tại; bản này chưa khóa nên vẫn có lỗi khi chạy đồng thời.
        Account from = AccountUtils.findAccount(accounts, command.fromAccountId());
        Account to = AccountUtils.findAccount(accounts, command.toAccountId());

        if (!from.getCurrency().equals(to.getCurrency())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Account currencies must match"
            );
        }

        // Chỉ chặn tràn số long. Bậc 1.1 chưa kiểm tra thiếu số dư.
        long fromBalance;
        long toBalance;

        try {
            fromBalance = Math.subtractExact(from.getBalance(), amount);
            toBalance = Math.addExact(to.getBalance(), amount);
        } catch (ArithmeticException e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Balance exceeds BIGINT range"
            );
        }

        // Bước 1: trừ tiền người gửi và lưu độc lập.
        from.setBalance(fromBalance);
        accounts.saveAndFlush(from);
        if (localFaults != null) {
            localFaults.ifAvailable(faults -> faults.failIfArmed(LocalTransferFaults.Point.AFTER_DEBIT));
        }

        // Bước 2: cộng tiền người nhận. Lỗi tại đây không rollback bước 1.
        to.setBalance(toBalance);
        accounts.saveAndFlush(to);

        // Bước 3: lưu dấu vết chuyển tiền sau khi cập nhật hai số dư.
        Transfer transfer = new Transfer();
        transfer.setId(UUID.randomUUID());
        transfer.setFromAccountId(from.getId());
        transfer.setToAccountId(to.getId());
        transfer.setAmount(amount);
        transfer.setCreatedAt(Instant.now());

        return TransferUtils.toResult(transfers.saveAndFlush(transfer));
    }

    @Override
    public TransferResult get(UUID id) {
        Transfer transfer = transfers.findById(id)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Transfer not found"
                ));

        return TransferUtils.toResult(transfer);
    }

    @Override
    public List<TransferResult> list() {
        return transfers.findAll()
                .stream()
                .map(TransferUtils::toResult)
                .toList();
    }

}
