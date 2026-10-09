package com.example.wallet.service;

import com.example.wallet.domain.Account;
import com.example.wallet.repository.AccountRepository;
import com.example.wallet.repository.TransferRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class LocalSeedResetServiceTest {
    @Test
    void removesTransfersBeforeAccountsAndRestoresExactV0Seed() {
        AccountRepository accounts = mock(AccountRepository.class);
        TransferRepository transfers = mock(TransferRepository.class);
        when(transfers.count()).thenReturn(2L);
        when(accounts.count()).thenReturn(4L);
        LocalSeedResetService service = new LocalSeedResetService(accounts, transfers);

        LocalSeedResetService.ResetResult result = service.reset();

        InOrder order = inOrder(transfers, accounts);
        order.verify(transfers).deleteAllInBatch();
        order.verify(transfers).flush();
        order.verify(accounts).deleteAllInBatch();
        order.verify(accounts).flush();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Iterable<Account>> seedCaptor = ArgumentCaptor.forClass(Iterable.class);
        verify(accounts).saveAllAndFlush(seedCaptor.capture());
        List<Account> seed = new ArrayList<>();
        seedCaptor.getValue().forEach(seed::add);

        assertThat(result).isEqualTo(new LocalSeedResetService.ResetResult(2, 4, 3));
        assertThat(seed).extracting(Account::getOwnerRef, Account::getCurrency, Account::getBalance)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("user-A", "VND", 100_000L),
                        org.assertj.core.groups.Tuple.tuple("user-B", "VND", 50_000L),
                        org.assertj.core.groups.Tuple.tuple("user-C", "VND", 0L)
                );
        assertThat(seed).extracting(account -> account.getId().toString())
                .containsExactly(
                        "00000000-0000-0000-0000-00000000000a",
                        "00000000-0000-0000-0000-00000000000b",
                        "00000000-0000-0000-0000-00000000000c"
                );
    }
}
