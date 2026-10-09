package com.example.wallet.infrastructure;

import com.example.wallet.domain.Account;
import java.util.*;

/** TODO: implement persistence yourself. Deliberately does not extend JpaRepository. */
public interface AccountRepository {
    Account save(Account account);
    Optional<Account> findById(UUID id);
    List<Account> findAll();
    void deleteById(UUID id);
}
