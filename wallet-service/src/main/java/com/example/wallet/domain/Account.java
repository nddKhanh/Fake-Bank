package com.example.wallet.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.UUID;

/** Minimal account structure from chapter 2, step 1.1. No balance or validation logic. */
@Getter @Setter @NoArgsConstructor
@Entity @Table(name = "accounts")
public class Account {
    @Id private UUID id;
    @Column(name = "owner_ref", nullable = false, length = 64) private String ownerRef;
    @Column(nullable = false, columnDefinition = "char(3)") private String currency;
    @Column(nullable = false) private long balance;
    @Column(name = "created_at", nullable = false) private Instant createdAt;
}
