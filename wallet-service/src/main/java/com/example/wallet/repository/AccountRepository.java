package com.example.wallet.repository;

import com.example.wallet.domain.Account;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.*;

/** TODO: implement persistence yourself. Deliberately does not extend JpaRepository. */
public interface AccountRepository extends JpaRepository<Account, UUID> {
}
