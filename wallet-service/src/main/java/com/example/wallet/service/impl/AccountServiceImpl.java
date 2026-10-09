package com.example.wallet.service.impl;

import com.example.wallet.domain.Account;
import com.example.wallet.dto.request.AccountResult;
import com.example.wallet.dto.request.CreateAccount;
import com.example.wallet.dto.request.UpdateAccount;
import com.example.wallet.repository.AccountRepository;
import com.example.wallet.service.AccountService;
import com.example.wallet.common.utils.AccountUtils;
import com.example.wallet.common.utils.MoneyUtils;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** CRUD tài khoản ở bậc 1.1 của chương 2. */
@Service
@Profile("db")
public class AccountServiceImpl implements AccountService {

    private final AccountRepository accounts;

    public AccountServiceImpl(AccountRepository accounts) {
        this.accounts = accounts;
    }

    @Override
    public AccountResult create(CreateAccount command) {
        AccountUtils.validateAccount(command.ownerRef(), command.currency());

        // Khởi tạo đầy đủ tài khoản bằng Lombok Builder trước khi lưu.
        Account account = Account.builder()
                .id(UUID.randomUUID())
                .ownerRef(command.ownerRef())
                .currency(command.currency())
                .balance(MoneyUtils.parseMoney(command.balance()))
                .createdAt(Instant.now())
                .build();

        return AccountUtils.toResult(accounts.save(account));
    }

    @Override
    public AccountResult get(UUID id) {
        return AccountUtils.toResult(AccountUtils.findAccount(accounts, id));
    }

    @Override
    public List<AccountResult> list() {
        return accounts.findAll()
                .stream()
                .map(AccountUtils::toResult)
                .toList();
    }

    @Override
    public AccountResult update(UUID id, UpdateAccount command) {
        // Bản CRUD ban đầu cho phép sửa trực tiếp số dư, chưa có sổ cái.
        Account existingAccount = AccountUtils.findAccount(accounts, id);
        AccountUtils.validateAccount(command.ownerRef(), command.currency());

        // Giữ thông tin định danh và thời điểm tạo khi cập nhật bằng builder.
        Account account = Account.builder()
                .id(existingAccount.getId())
                .ownerRef(command.ownerRef())
                .currency(command.currency())
                .balance(MoneyUtils.parseMoney(command.balance()))
                .createdAt(existingAccount.getCreatedAt())
                .build();

        return AccountUtils.toResult(accounts.save(account));
    }

    @Override
    public void delete(UUID id) {
        // Khóa ngoại trong database sẽ chặn xóa tài khoản đã có giao dịch.
        accounts.delete(AccountUtils.findAccount(accounts, id));
    }

}
