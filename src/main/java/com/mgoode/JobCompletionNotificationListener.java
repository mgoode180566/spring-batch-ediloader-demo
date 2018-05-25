package com.mgoode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by michaelgoode on 20/03/2018.
 */
@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;

    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to verify the results");

            List<SkuLine> results = jdbcTemplate.query("SELECT * FROM edi_skubatch", new RowMapper<SkuLine>() {
                @Override
                public SkuLine mapRow(ResultSet rs, int row) throws SQLException {

                    SkuLine skuLine = new SkuLine();
                    skuLine.setCONT_NO(rs.getString("cont_no"));
                    skuLine.setCUST_NO(rs.getString("Cust_no"));
                    return skuLine;

                }
            });

            for (SkuLine skuLine : results) {
                //log.info("Found contract <" + skuLine.getCONT_NO() + ", " + skuLine.getCUST_NO() + skuLine.getF120() + "> in the database.");
            }

        }
    }


}
