package com.mgoode;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Configuration
public class EDILoaderBatchConfiguration {

    final String fieldNames = "PK_SKUBATCHID,FK_fileconfig_fileconfigID,ImportTime,Status,BatchExtend_Unkey,OrderID,cont_no,Cust_no,Ord_from,BillTo,ShipTo,SO_Remark,File_name,Prod_id,Sty_no,Factory,Item_code,Lot_no,Lot_ref,Style,Color,Size_des,Qty_ord,Qty_so,Unit,Contents,Price,LookUp,Currency,Size,Bar_UPC,Retail_Pri,Season,Style_Desc,Style_num,Bar_EAN,Bar_1,Bar_2,Buy_Season,Buy_Month,Buy_year,Care1,Care2,Care3,Care4,Care5,Care6,CareInst1,CareInst2,CareInst3,CareInst4,CareInst5,CareInst6,Color1,Color2,Color3,Country,Dept,Desc1,Desc2,Desc3,Desc4,Desc5,Desc6,DIV,HT_Code1,HT_Code2,HT_Code3,HT_Code4,HT_Code5,HT_Code6,Fiber1,Fiber2,Fiber3,Fiber4,Fiber5,Fiber6,Group1,Group2,Line1,Line2,Logo1,Logo2,Logo3,Logo4,Logo5,Logo6,Logo7,Logo8,Logo9,Cur_Cnty1,Cur_Cnty2,Cur_Cnty3,Cur_Cnty4,Cur_Cnty5,Cur_Cnty6,Prt_Price1,Prt_Price2,Prt_Price3,Prt_Price4,Prt_Price5,Prt_Price6,Prt_Size1,Prt_size2,Prt_size3,Prt_size4,Prt_size5,Prt_size6,Remark1,Remark2,Remark3,Style_Des1,Style_Des2,Style_Des3,Style_Des4,Style_Des5,Style_Des6,Mat1,Mat2,Mat3,Mat4,Mat5,Mat6,rowCreateTime,rowUpdateTime,rowLastUpdateTimestamp,SysVar0,SysVar1,istest,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12,F13,F14,F15,F16,F17,F18,F19,F20,F21,F22,F23,F24,F25,F26,F27,F28,F29,F30,F31,F32,F33,F34,F35,F36,F37,F38,F39,F40,F41,F42,F43,F44,F45,F46,F47,F48,F49,F50,F51,F52,F53,F54,F55,F56,F57,F58,F59,F60,F61,F62,F63,F64,F65,F66,F67,F68,F69,F70,F71,F72,F73,F74,F75,F76,F77,F78,F79,F80,F81,F82,F83,F84,F85,F86,F87,F88,F89,F90,F91,F92,F93,F94,F95,F96,F97,F98,F99,F100,F101,F102,F103,F104,F105,F106,F107,F108,F109,F110,F111,F112,F113,F114,F115,F116,F117,F118,F119,F120";

    final String insertSQL = "insert into EDI_SKUBATCH_WORKING (PK_SKUBATCHID,FK_FILECONFIG_FILECONFIGID,IMPORTTIME,STATUS,BATCHEXTEND_UNKEY,ORDERID,CONT_NO,CUST_NO,ORD_FROM,BILLTO,SHIPTO,SO_REMARK,FILE_NAME,PROD_ID,STY_NO,FACTORY,ITEM_CODE,LOT_NO,LOT_REF,STYLE,COLOR,SIZE_DES,QTY_ORD,QTY_SO,UNIT,CONTENTS,PRICE,LOOKUP,CURRENCY,SIZE,BAR_UPC,RETAIL_PRI,SEASON,STYLE_DESC,STYLE_NUM,BAR_EAN,BAR_1,BAR_2,BUY_SEASON,BUY_MONTH,BUY_YEAR,CARE1,CARE2,CARE3,CARE4,CARE5,CARE6,CAREINST1,CAREINST2,CAREINST3,CAREINST4,CAREINST5,CAREINST6,COLOR1,COLOR2,COLOR3,COUNTRY,DEPT,DESC1,DESC2,DESC3,DESC4,DESC5,DESC6,DIV,HT_CODE1,HT_CODE2,HT_CODE3,HT_CODE4,HT_CODE5,HT_CODE6,FIBER1,FIBER2,FIBER3,FIBER4,FIBER5,FIBER6,GROUP1,GROUP2,LINE1,LINE2,LOGO1,LOGO2,LOGO3,LOGO4,LOGO5,LOGO6,LOGO7,LOGO8,LOGO9,CUR_CNTY1,CUR_CNTY2,CUR_CNTY3,CUR_CNTY4,CUR_CNTY5,CUR_CNTY6,PRT_PRICE1,PRT_PRICE2,PRT_PRICE3,PRT_PRICE4,PRT_PRICE5,PRT_PRICE6,PRT_SIZE1,PRT_SIZE2,PRT_SIZE3,PRT_SIZE4,PRT_SIZE5,PRT_SIZE6,REMARK1,REMARK2,REMARK3,STYLE_DES1,STYLE_DES2,STYLE_DES3,STYLE_DES4,STYLE_DES5,STYLE_DES6,MAT1,MAT2,MAT3,MAT4,MAT5,MAT6,ROWCREATETIME,ROWUPDATETIME,ROWLASTUPDATETIMESTAMP,SYSVAR0,SYSVAR1,ISTEST,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12,F13,F14,F15,F16,F17,F18,F19,F20,F21,F22,F23,F24,F25,F26,F27,F28,F29,F30,F31,F32,F33,F34,F35,F36,F37,F38,F39,F40,F41,F42,F43,F44,F45,F46,F47,F48,F49,F50,F51,F52,F53,F54,F55,F56,F57,F58,F59,F60,F61,F62,F63,F64,F65,F66,F67,F68,F69,F70,F71,F72,F73,F74,F75,F76,F77,F78,F79,F80,F81,F82,F83,F84,F85,F86,F87,F88,F89,F90,F91,F92,F93,F94,F95,F96,F97,F98,F99,F100,F101,F102,F103,F104,F105,F106,F107,F108,F109,F110,F111,F112,F113,F114,F115,F116,F117,F118,F119,F120)"
            + " values (0,:FK_FILECONFIG_FILECONFIGID,:IMPORTTIME,:STATUS,:BATCHEXTEND_UNKEY,:ORDERID,:CONT_NO,:CUST_NO,:ORD_FROM,:BILLTO,:SHIPTO,:SO_REMARK,:FILE_NAME,:PROD_ID,:STY_NO,:FACTORY,:ITEM_CODE,:LOT_NO,:LOT_REF,:STYLE,:COLOR,:SIZE_DES,:QTY_ORD,:QTY_SO,:UNIT,:CONTENTS,:PRICE,:LOOKUP,:CURRENCY,:SIZE,:BAR_UPC,:RETAIL_PRI,:SEASON,:STYLE_DESC,:STYLE_NUM,:BAR_EAN,:BAR_1,:BAR_2,:BUY_SEASON,:BUY_MONTH,:BUY_YEAR,:CARE1,:CARE2,:CARE3,:CARE4,:CARE5,:CARE6,:CAREINST1,:CAREINST2,:CAREINST3,:CAREINST4,:CAREINST5,:CAREINST6,:COLOR1,:COLOR2,:COLOR3,:COUNTRY,:DEPT,:DESC1,:DESC2,:DESC3,:DESC4,:DESC5,:DESC6,:DIV,:HT_CODE1,:HT_CODE2,:HT_CODE3,:HT_CODE4,:HT_CODE5,:HT_CODE6,:FIBER1,:FIBER2,:FIBER3,:FIBER4,:FIBER5,:FIBER6,:GROUP1,:GROUP2,:LINE1,:LINE2,:LOGO1,:LOGO2,:LOGO3,:LOGO4,:LOGO5,:LOGO6,:LOGO7,:LOGO8,:LOGO9,:CUR_CNTY1,:CUR_CNTY2,:CUR_CNTY3,:CUR_CNTY4,:CUR_CNTY5,:CUR_CNTY6,:PRT_PRICE1,:PRT_PRICE2,:PRT_PRICE3,:PRT_PRICE4,:PRT_PRICE5,:PRT_PRICE6,:PRT_SIZE1,:PRT_SIZE2,:PRT_SIZE3,:PRT_SIZE4,:PRT_SIZE5,:PRT_SIZE6,:REMARK1,:REMARK2,:REMARK3,:STYLE_DES1,:STYLE_DES2,:STYLE_DES3,:STYLE_DES4,:STYLE_DES5,:STYLE_DES6,:MAT1,:MAT2,:MAT3,:MAT4,:MAT5,:MAT6,:ROWCREATETIME,:ROWUPDATETIME,:ROWLASTUPDATETIMESTAMP,:SYSVAR0,:SYSVAR1,:ISTEST,:F1,:F2,:F3,:F4,:F5,:F6,:F7,:F8,:F9,:F10,:F11,:F12,:F13,:F14,:F15,:F16,:F17,:F18,:F19,:F20,:F21,:F22,:F23,:F24,:F25,:F26,:F27,:F28,:F29,:F30,:F31,:F32,:F33,:F34,:F35,:F36,:F37,:F38,:F39,:F40,:F41,:F42,:F43,:F44,:F45,:F46,:F47,:F48,:F49,:F50,:F51,:F52,:F53,:F54,:F55,:F56,:F57,:F58,:F59,:F60,:F61,:F62,:F63,:F64,:F65,:F66,:F67,:F68,:F69,:F70,:F71,:F72,:F73,:F74,:F75,:F76,:F77,:F78,:F79,:F80,:F81,:F82,:F83,:F84,:F85,:F86,:F87,:F88,:F89,:F90,:F91,:F92,:F93,:F94,:F95,:F96,:F97,:F98,:F99,:F100,:F101,:F102,:F103,:F104,:F105,:F106,:F107,:F108,:F109,:F110,:F111,:F112,:F113,:F114,:F115,:F116,:F117,:F118,:F119,:F120)";

    int _CHUNK_SIZE = 100;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private SkuLineProcessor skuLineProcessor;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Bean(name = "dataSource")
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource getDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public LineMapper<SkuLine> getMapForceDocumentSkuLineMapper() {
        System.out.println("run skuLineLineMapper");
        String[] fields = fieldNames.toUpperCase().split(DelimitedLineTokenizer.DELIMITER_COMMA);
        // We are reading in a CSV file, so set a TAB as the delimiter.
        MapForceDelimitedLineTokenizer lineTokenizer = new MapForceDelimitedLineTokenizer();
        lineTokenizer.setDelimiter(DelimitedLineTokenizer.DELIMITER_TAB);
        lineTokenizer.setNames(fields);
        // The lines in our file map to the SkuLine class.
        BeanWrapperFieldSetMapper<SkuLine> fieldSetMapper = new BeanWrapperFieldSetMapper<SkuLine>();
        fieldSetMapper.setTargetType(SkuLine.class);
        // Use the field delimiter and field set mapper to define our line mapper.
        DefaultLineMapper<SkuLine> lineMapper = new DefaultLineMapper<SkuLine>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public FlatFileItemReader<SkuLine> getMapForceDocumentReader() {
        final int NO_OF_LINES_TO_SKIP = 1; // skip the header field record in the file
        runMapping(); // run the respective mapping
        System.out.println("run MapForce document reader");
        try {
            FlatFileItemReader<SkuLine> flatFileItemReader = new FlatFileItemReader<>();
            //flatFileItemReader.setResource(new ClassPathResource("PLMLABELCLA_20180420_2018_04_20_09_01_24_925604e3-43cf-4a5a-9dd1-96df9ac1f905.import")); // input file
            flatFileItemReader.setResource(new FileSystemResource("C:\\MS Mapping\\output\\PLMLABELCLA_20180420_2018_04_20_09_01_24_9547a056-7067-42a5-a7fe-ed46b29eb693.import"));
            flatFileItemReader.setLineMapper(getMapForceDocumentSkuLineMapper());
            flatFileItemReader.setLinesToSkip(NO_OF_LINES_TO_SKIP);
            flatFileItemReader.setStrict(true);
            flatFileItemReader.setEncoding(StandardCharsets.UTF_16LE.name()); // UTF-16, little endian encoding
            flatFileItemReader.setRecordSeparatorPolicy(new SimpleRecordSeparatorPolicy());
            return flatFileItemReader;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    private void runMapping() {
        try
        {
            String command = "C:\\MS Mapping\\Mapping.exe";
            Runtime run  = Runtime.getRuntime();
            Process proc = run.exec(command);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Bean
    public ItemWriter<SkuLine> getSkuLineWriter() {
        System.out.println("run Sku line writer");
        JdbcBatchItemWriter<SkuLine> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setDataSource(getDataSource());
        writer.setSql(insertSQL);
        return writer;
    }

    @Bean
    public SkuLineProcessor getSkuLineProcessor() {
        return skuLineProcessor;
    }

    @Bean
    public Job skuJob(JobCompletionNotificationListener skuCompletionNotificationListener) {
        Job job = null;

        job = jobBuilderFactory.get("ediLoaderProcessOrders")
                .incrementer(incrementer())
                .listener(skuCompletionNotificationListener)
                .start(skuWriteMappingStep())
                .next(skuUpdateLookupStep())
                .next(skuMoveStep())
                .next(skuClearWorkingStep())
                .build();
        return job;
    }

    @Bean
    public Tasklet getTasklet() {
        StringBuilder sb = new StringBuilder();
        sb.append("update EDI_SKUBATCH_WORKING set PROD_ID = ( select UValue1 From EDI_LOOKUP_VALUE where FK_LOOKUP_CONDITIONID = 140 and ITEM_CODE = CVALUE1 ), STY_NO = ( select UValue2 From EDI_LOOKUP_VALUE where FK_LOOKUP_CONDITIONID = 140 and ITEM_CODE = CVALUE1 ), LOOKUP = ( select UValue3 From EDI_LOOKUP_VALUE where FK_LOOKUP_CONDITIONID = 140 and ITEM_CODE = CVALUE1 );");
        sb.append("update EDI_SKUBATCH_WORKING set F80 = F15||F16||F17;");
        sb.append("update EDI_SKUBATCH_WORKING set Factory = F78;");

        UpdateSQLTasklet updateSQLTasklet = new UpdateSQLTasklet();
        updateSQLTasklet.setSql("UPDATE EDI_SKUBATCH_WORKING SET F120='COMPLETE'");
        return updateSQLTasklet;
    }

    @Bean
    public Tasklet getTaskletMoveToSkuBatch() {
        String sql = "INSERT INTO EDI_SKUBATCH ( SELECT * FROM EDI_SKUBATCH_WORKING )";
        UpdateSQLTasklet updateSQLTasklet = new UpdateSQLTasklet();
        updateSQLTasklet.setSql(sql);
        return updateSQLTasklet;
    }

    @Bean
    public Tasklet getTaskletClearWorkingSkuBatch() {
        String sql = "DELETE FROM EDI_SKUBATCH_WORKING";
        System.out.println(sql);
        UpdateSQLTasklet updateSQLTasklet = new UpdateSQLTasklet();
        updateSQLTasklet.setSql(sql);
        return updateSQLTasklet;
    }

    @Bean
    protected Step skuWriteMappingStep() {
        Step step = null;
        step = stepBuilderFactory.get("skuWriteMappingStep").<SkuLine, SkuLine>chunk(5)
                .reader(getMapForceDocumentReader())
                .processor(getSkuLineProcessor())
                .writer(getSkuLineWriter())
                .build();
        return step;
    }

    @Bean
    protected Step skuMoveStep() {
        Step step = null;
        step = stepBuilderFactory.get("skuMoveStep")
                .tasklet(getTaskletMoveToSkuBatch())
                .build();
        return step;
    }


    @Bean
    protected Step skuClearWorkingStep() {
        Step step = null;
        step = stepBuilderFactory.get("skuClearWorkingStep")
                .tasklet(getTaskletClearWorkingSkuBatch())
                .build();
        return step;
    }

    @Bean
    protected Step skuUpdateLookupStep() {
        Step step = null;
        step = stepBuilderFactory.get("skuUpdateStep")
                .tasklet(getTasklet())
                .build();
        return step;
    }

    @Bean
    JobParametersIncrementer incrementer() {
        // Simple Spring-provided JobParametersIncrementer implementation that increments the run id.
        return new RunIdIncrementer();
    }

}
