package com.example.wallet;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("lab")
class LabScaffoldTests {
    @Autowired MockMvc mvc;
    @Test void labEndpointsAreStubsEvenWhenProfileEnabled() throws Exception {
        mvc.perform(post("/api/lab/scenarios/A1/runs").contentType("application/json").content("{\"seed\":42}"))
            .andExpect(status().isNotImplemented());
        mvc.perform(post("/api/lab/reset").contentType("application/json").content("{\"confirmed\":true}"))
            .andExpect(status().isNotImplemented());
        mvc.perform(put("/api/lab/chaos/lose-response").contentType("application/json").content("{\"enabled\":true}"))
            .andExpect(status().isNotImplemented());
    }
}
