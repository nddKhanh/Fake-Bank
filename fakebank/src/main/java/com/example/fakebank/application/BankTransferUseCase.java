package com.example.fakebank.application;

import java.util.UUID;

/** TODO: implement idempotency, permanent rejection, chaos and callback outbox according to stage 4. */
public interface BankTransferUseCase {
    record Command(String clientRequestId, String accountNumber, String amount, String currency) {}
    record Result(UUID id, String bankReference, String clientRequestId, String status, String failureCode) {}
    Result transfer(Command command);
    Result inquiry(String clientRequestId);
}
