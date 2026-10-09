package com.example.wallet.common.utils;

import com.example.wallet.domain.Transfer;
import com.example.wallet.service.TransferService.TransferResult;

/** Chuyển entity giao dịch sang dữ liệu phản hồi API. */
public final class TransferUtils {

    private TransferUtils() {
    }

    public static TransferResult toResult(Transfer transfer) {
        return new TransferResult(
                transfer.getId(),
                transfer.getFromAccountId(),
                transfer.getToAccountId(),
                Long.toString(transfer.getAmount()),
                transfer.getCreatedAt()
        );
    }
}
