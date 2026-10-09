package com.example.wallet.repository;

import com.example.wallet.domain.Account;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.*;

/** Basic account persistence for chapter 2, step 1.1. */
public interface AccountRepository extends JpaRepository<Account, UUID> {
}
