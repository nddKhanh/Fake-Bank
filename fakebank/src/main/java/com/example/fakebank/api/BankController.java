package com.example.fakebank.api;

import com.example.fakebank.application.BankTransferUseCase.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Deliberately empty backend endpoints. No balance changes or simulated success responses. */
@RestController
@RequestMapping("/bank")
public class BankController {
    @PostMapping("/transfers") public Result transfer(@RequestBody Command command) {
        throw new ResponseStatusException(HttpStatus.NOT_IMPLEMENTED,"TODO: BankTransferUseCase.transfer");
    }
    @GetMapping("/transfers/{clientRequestId}") public Result inquiry(@PathVariable String clientRequestId) {
        throw new ResponseStatusException(HttpStatus.NOT_IMPLEMENTED,"TODO: BankTransferUseCase.inquiry");
    }
}
