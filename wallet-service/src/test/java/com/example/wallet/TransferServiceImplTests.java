package com.example.wallet;

import com.example.wallet.domain.*;
import com.example.wallet.repository.*;
import com.example.wallet.service.TransferService.CreateTransfer;
import com.example.wallet.service.impl.TransferServiceImpl;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TransferServiceImplTests {
    @Test void sequentialTransferMatchesStep11() {
        var accounts=mock(AccountRepository.class); var transfers=mock(TransferRepository.class);
        var a=account(100000);var b=account(50000);
        when(accounts.findById(a.getId())).thenReturn(Optional.of(a));
        when(accounts.findById(b.getId())).thenReturn(Optional.of(b));
        when(transfers.saveAndFlush(any(Transfer.class))).thenAnswer(i->i.getArgument(0));
        var result=new TransferServiceImpl(accounts,transfers).create(new CreateTransfer(a.getId(),b.getId(),"30000"));
        assertEquals(70000,a.getBalance());assertEquals(80000,b.getBalance());assertEquals("30000",result.amount());
        var order=inOrder(accounts,transfers);order.verify(accounts).saveAndFlush(a);order.verify(accounts).saveAndFlush(b);order.verify(transfers).saveAndFlush(any(Transfer.class));
    }
    @Test void secondWriteFailureStopsBeforeTransferIsRecorded() {
        var accounts=mock(AccountRepository.class);var transfers=mock(TransferRepository.class);
        var a=account(100000);var b=account(50000);
        when(accounts.findById(a.getId())).thenReturn(Optional.of(a));when(accounts.findById(b.getId())).thenReturn(Optional.of(b));
        when(accounts.saveAndFlush(b)).thenThrow(new IllegalStateException("credit fails"));
        assertThrows(IllegalStateException.class,()->new TransferServiceImpl(accounts,transfers).create(new CreateTransfer(a.getId(),b.getId(),"30000")));
        verify(accounts).saveAndFlush(a);verify(transfers,never()).saveAndFlush(any());
    }
    private Account account(long balance) {var a=new Account();a.setId(UUID.randomUUID());a.setCurrency("VND");a.setBalance(balance);return a;}
}
