package com.example.wallet.service;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicReference;

/** One-shot local failpoints used to expose partial writes in the intentionally naive V0 flow. */
@Service
@Profile("db-seed")
public class LocalTransferFaults {
    public enum Point { NONE, AFTER_DEBIT }

    private final AtomicReference<Point> next = new AtomicReference<>(Point.NONE);

    public void arm(Point point) {
        if (point == null || point == Point.NONE) throw new IllegalArgumentException("A concrete fault point is required");
        next.set(point);
    }

    public void failIfArmed(Point point) {
        if (next.compareAndSet(point, Point.NONE)) {
            throw new InjectedTransferFailure("Injected local failure at " + point);
        }
    }

    public void clear() {
        next.set(Point.NONE);
    }

    public static final class InjectedTransferFailure extends RuntimeException {
        public InjectedTransferFailure(String message) { super(message); }
    }
}
