package com.example.fakebank.domain;

public enum ChaosMode {
    DELAY,
    ERROR_5XX,
    LOSE_RESPONSE,
    CALLBACK_FAIL_AFTER_SUCCESS,
    CALLBACK_DUPLICATE,
    CALLBACK_LATE,
    DOWNTIME,
    REJECT_ACCOUNT
}
