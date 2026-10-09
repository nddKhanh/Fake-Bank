package com.example.wallet.infrastructure;

import com.example.wallet.domain.Transfer;
import java.util.*;

/** TODO: persist and query transfers; no SQL, locking or transaction implementation provided. */
public interface TransferRepository {
    Transfer insert(Transfer transfer);
    Optional<Transfer> findById(UUID id);
    List<Transfer> findAll();
}
