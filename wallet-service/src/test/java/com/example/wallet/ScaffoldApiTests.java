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
 @Test void crudEndpointsRemainUnimplemented() throws Exception {
  mvc.perform(get("/accounts")).andExpect(status().isNotImplemented());
  mvc.perform(get("/transfers")).andExpect(status().isNotImplemented());
  mvc.perform(post("/transfers").contentType("application/json").content("{\"amount\":\"100\"}")).andExpect(status().isNotImplemented());
 }
 @Test void observationAndFrontendAreOutsideBackend() throws Exception {
  mvc.perform(get("/api/ops/capabilities")).andExpect(status().isNotFound());
  mvc.perform(post("/api/lab/reset")).andExpect(status().isNotFound());
  mvc.perform(get("/index.html")).andExpect(status().isNotFound());
 }
}
