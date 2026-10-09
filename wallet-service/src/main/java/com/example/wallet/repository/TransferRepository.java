package com.example.wallet.repository;

import com.example.wallet.domain.Transfer;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.*;

/** Basic transfer persistence; no custom locking or queries at step 1.1. */
public interface TransferRepository extends JpaRepository<Transfer, UUID> {
}
