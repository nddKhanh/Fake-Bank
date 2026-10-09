package com.example.wallet;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class ScaffoldApiTests {
    @Autowired MockMvc mvc;
    @Test void uiServedWithoutDatabase() throws Exception {
        mvc.perform(get("/index.html")).andExpect(status().isOk()).andExpect(content().contentTypeCompatibleWith("text/html"));
    }
    @Test void readsExplicitlyReportNotImplemented() throws Exception {
        for(String endpoint : new String[]{"overview","transfers","accounts","queues","reconciliation","runs"})
            mvc.perform(get("/api/ops/"+endpoint)).andExpect(status().isNotImplemented()).andExpect(jsonPath("$.code").value("NOT_IMPLEMENTED"));
    }
    @Test void capabilitiesDoNotPretendToRunTests() throws Exception {
        mvc.perform(get("/api/ops/capabilities")).andExpect(status().isOk())
            .andExpect(jsonPath("$.backendImplemented").value(false)).andExpect(jsonPath("$.labEnabled").value(false));
    }
    @Test void labUnavailableOutsideLabProfile() throws Exception {
        mvc.perform(post("/api/lab/reset").contentType("application/json").content("{\"confirmed\":true}"))
            .andExpect(status().isNotFound());
    }
    @Test void transferScaffoldCannotMoveMoney() throws Exception {
        mvc.perform(post("/transfers").header("Idempotency-Key","test-key").contentType("application/json")
            .content("{\"type\":\"INTERNAL\",\"amount\":\"100\",\"currency\":\"VND\"}"))
            .andExpect(status().isNotImplemented());
    }
}
