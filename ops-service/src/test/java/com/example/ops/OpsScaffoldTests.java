package com.example.ops;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
@SpringBootTest
@AutoConfigureMockMvc
class OpsScaffoldTests {
 @Autowired MockMvc mvc;
 @Test void queriesRemainStubs() throws Exception {
  for(String endpoint:new String[]{"overview","transfers","accounts","queues","reconciliation","runs"})
   mvc.perform(get("/api/ops/"+endpoint)).andExpect(status().isNotImplemented()).andExpect(jsonPath("$.code").value("NOT_IMPLEMENTED"));
 }
 @Test void capabilitiesAndIsolation() throws Exception {
  mvc.perform(get("/api/ops/capabilities")).andExpect(status().isOk()).andExpect(jsonPath("$.backendImplemented").value(false));
  mvc.perform(post("/api/lab/reset")).andExpect(status().isNotFound());
  mvc.perform(get("/accounts")).andExpect(status().isNotFound());
 }
}
