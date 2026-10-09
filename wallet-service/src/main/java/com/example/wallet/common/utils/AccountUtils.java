package com.example.wallet.common.utils;

import com.example.wallet.domain.Account;
import com.example.wallet.dto.request.AccountResult;
import com.example.wallet.repository.AccountRepository;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

/** Các hàm hỗ trợ tài khoản dùng chung cho service CRUD. */
public final class AccountUtils {

    private AccountUtils() {
    }

    public static Account findAccount(AccountRepository accounts, UUID id) {
        return accounts.findById(id)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Account not found"
                ));
    }

    public static void validateAccount(String owner, String currency) {
        // Kiểm tra dữ liệu phù hợp với các cột trong schema V0.
        if (owner == null || owner.isBlank() || owner.length() > 64) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "ownerRef must contain 1-64 characters"
            );
        }

        if (currency == null || !currency.matches("[A-Z]{3}")) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "currency must be a three-letter uppercase code"
            );
        }

    }

    public static AccountResult toResult(Account account) {
        // Trả tiền dưới dạng chuỗi để JavaScript không mất độ chính xác.
        return new AccountResult(
                account.getId(),
                account.getOwnerRef(),
                account.getCurrency(),
                Long.toString(account.getBalance()),
                account.getCreatedAt()
        );
    }
}
