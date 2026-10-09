package com.example.wallet.repository;

import com.example.wallet.domain.Transfer;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.*;

/** TODO: persist and query transfers; no SQL, locking or transaction implementation provided. */
public interface TransferRepository extends JpaRepository<Transfer, UUID> {
}
