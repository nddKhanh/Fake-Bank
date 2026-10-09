package com.example.fakebank;

import com.example.fakebank.api.BankController;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class BankControllerScaffoldTests {
    @Test void bankScaffoldDoesNotPretendToTransferOrInquire() throws Exception {
        var mvc=MockMvcBuilders.standaloneSetup(new BankController()).build();
        mvc.perform(post("/bank/transfers").contentType("application/json")
            .content("{\"clientRequestId\":\"key\",\"accountNumber\":\"123\",\"amount\":\"100\",\"currency\":\"VND\"}"))
            .andExpect(status().isNotImplemented());
        mvc.perform(get("/bank/transfers/key")).andExpect(status().isNotImplemented());
    }
}
