package com.example.fakebank.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "bank_transactions")
public class BankTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "bank_reference", nullable = false, unique = true, length = 50)
    private String bankReference;

    @Column(name = "client_request_id", nullable = false, unique = true, length = 100)
    private String clientRequestId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private BankTransactionType type;

    @Column(name = "account_number", nullable = false, length = 50)
    private String accountNumber;

    @Column(nullable = false)
    private long amount;

    @Column(nullable = false, length = 3, columnDefinition = "char(3)")
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private BankTransactionStatus status;

    @Column(name = "failure_code", length = 50)
    private String failureCode;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "completed_at")
    private Instant completedAt;
}
