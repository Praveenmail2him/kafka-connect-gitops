package com.example.orders;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = BulkOrderController.class)
class BulkOrderControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private BulkOrderService bulk;

    @Test
    void rejectsMoreThanTenBillion() throws Exception {
        mvc.perform(post("/api/orders/bulk")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"count\":10000000001}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void startsAVolumeRun() throws Exception {
        when(bulk.start(1000)).thenReturn(new BulkOrderService.BulkStatus("job-1", 1000, 0, 0, "RUNNING", 0, ""));

        mvc.perform(post("/api/orders/bulk")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"count\":1000}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andExpect(jsonPath("$.total").value(1000));

        verify(bulk).start(1000);
    }
}
