package com.example.wallet.api;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_IMPLEMENTED)
public class NotImplemented extends RuntimeException {
    public NotImplemented(String feature) { super("TODO: " + feature); }
}
