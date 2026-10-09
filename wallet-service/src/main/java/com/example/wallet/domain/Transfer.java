package com.example.wallet.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.UUID;

/** Minimal transfer structure from step 1.1; later stages add status, ledger and idempotency. */
@Getter @Setter @NoArgsConstructor
@Entity @Table(name = "transfers")
public class Transfer {
    @Id private UUID id;
    @Column(name = "from_account_id", nullable = false) private UUID fromAccountId;
    @Column(name = "to_account_id", nullable = false) private UUID toAccountId;
    @Column(nullable = false) private long amount;
    @Column(name = "created_at", nullable = false) private Instant createdAt;
}
