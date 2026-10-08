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

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "bank_accounts")
public class BankAccount {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "account_number", nullable = false, unique = true, length = 50)
    private String accountNumber;

    @Column(name = "holder_name", nullable = false, length = 100)
    private String holderName;

    @Column(nullable = false)
    private long balance;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private BankAccountStatus status = BankAccountStatus.ACTIVE;
}
