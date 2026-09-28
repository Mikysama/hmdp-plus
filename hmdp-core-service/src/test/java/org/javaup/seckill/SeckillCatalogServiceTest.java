package org.javaup.seckill;

import org.javaup.context.DelayQueueContext;
import org.javaup.dto.*;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SeckillCatalogServiceTest {
 JdbcTemplate jdbc; SeckillCatalogService catalog; SeckillTransactions tx; DelayQueueContext delays;
 @BeforeEach void setup() throws Exception {
  var ds=new DriverManagerDataSource("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MySQL;DB_CLOSE_DELAY=-1","sa","");jdbc=new JdbcTemplate(ds);
  jdbc.execute("CREATE TABLE tb_voucher(id BIGINT PRIMARY KEY,shop_id BIGINT,title VARCHAR(255),sub_title VARCHAR(255),rules VARCHAR(1024),pay_value BIGINT,actual_value BIGINT,type INT,status INT,create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
  jdbc.execute("CREATE TABLE tb_seckill_voucher(id BIGINT,voucher_id BIGINT PRIMARY KEY,init_stock INT,stock INT,reserved_stock INT,sold_stock INT,version BIGINT,rule_version BIGINT,admission_epoch BIGINT,admission_state VARCHAR(16),projection_seq BIGINT,begin_time TIMESTAMP,end_time TIMESTAMP,allowed_levels VARCHAR(64),min_level INT,create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
  String ddl=Files.readString(Path.of("../sql/v2/new_tables.sql")).replace("___N__","").replaceAll("(?i) COLLATE utf8mb4_bin", "").replaceAll("(?i) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", "");
  int idx=0; for(String statement:ddl.split(";"))if(!statement.isBlank())jdbc.execute(statement.replaceAll("(?i)(KEY) `([a-z_]+)`", "$1 `$2_"+(idx++)+"`"));
  var store=new SeckillStore(jdbc);tx=new SeckillTransactions(store,new TransactionTemplate(new DataSourceTransactionManager(ds)),60);
  var ids=mock(SnowflakeIdGenerator.class);when(ids.nextId()).thenReturn(101L,102L,103L,104L);delays=mock(DelayQueueContext.class);
  catalog=new SeckillCatalogService(store,tx,ids,delays,120);
 }
 SeckillVoucherDto valid(){return new SeckillVoucherDto().setShopId(1L).setTitle("券").setSubTitle("限时").setPayValue(100L).setActualValue(200L).setType(1).setStatus(1).setStock(10).setBeginTime(LocalDateTime.now().plusHours(1)).setEndTime(LocalDateTime.now().plusHours(2));}
 @Test void createsPausedConservedInventoryAndDurableReminderWithoutNetwork(){long id=catalog.addSeckill(valid());var view=catalog.get(id);assertEquals("PAUSED",view.get("admissionState"));assertEquals(10,((Number)view.get("stock")).intValue());assertEquals(0,((Number)view.get("reservedStock")).intValue());assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM tb_seckill_outbox WHERE event_type='REMINDER'",Integer.class));verifyNoInteractions(delays);}
 @Test void rejectsInvalidLevelAndTimeWithoutPartialRows(){var bad=valid().setAllowedLevels("1,bad");assertThrows(SeckillFailure.class,()->catalog.addSeckill(bad));var time=valid();time.setEndTime(time.getBeginTime());assertThrows(SeckillFailure.class,()->catalog.addSeckill(time));assertEquals(0,jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher",Integer.class));}
 @Test void updateChecksVersionAndMergedMoneyThenFencesAdmission(){long id=catalog.addSeckill(valid());jdbc.update("UPDATE tb_seckill_voucher SET admission_state='OPEN' WHERE voucher_id=?",id);var invalid=new UpdateSeckillVoucherDto().setVoucherId(id).setExpectedVersion(0L).setPayValue(201L);assertThrows(SeckillFailure.class,()->catalog.update(invalid));assertEquals(100L,((Number)catalog.get(id).get("payValue")).longValue());catalog.update(new UpdateSeckillVoucherDto().setVoucherId(id).setExpectedVersion(0L).setStatus(2));var view=catalog.get(id);assertEquals(2,((Number)view.get("status")).intValue());assertEquals("REBUILDING",view.get("admissionState"));assertEquals(1L,((Number)view.get("ruleVersion")).longValue());assertThrows(SeckillFailure.class,()->catalog.update(new UpdateSeckillVoucherDto().setVoucherId(id).setExpectedVersion(0L).setStatus(1)));}
 @Test void shrinkingToZeroIsAllowedAndAdjustmentIsIdempotent(){long id=catalog.addSeckill(valid());jdbc.update("UPDATE tb_seckill_voucher SET admission_state='OPEN' WHERE voucher_id=?",id);var dto=new UpdateSeckillVoucherStockDto().setVoucherId(id).setAdjustmentId("resize").setExpectedVersion(0L).setInitStock(0);catalog.adjust(dto);catalog.adjust(dto);assertEquals(0,((Number)catalog.get(id).get("stock")).intValue());}
 @Test void ordinaryVoucherCannotImpersonateSeckill(){var dto=new VoucherDto().setShopId(1L).setTitle("t").setSubTitle("s").setRules("r").setPayValue(1L).setActualValue(2L).setType(1).setStatus(1);assertThrows(SeckillFailure.class,()->catalog.addOrdinary(dto));}
 @Test void reminderIsPersistedOnceForCurrentSubscriberAndOldScheduleIsIgnored(){long id=catalog.addSeckill(valid());jdbc.update("UPDATE tb_seckill_voucher SET admission_state='OPEN' WHERE voucher_id=?",id);tx.subscribe(id,9,true);var begin=(LocalDateTime)catalog.get(id).get("beginTime");assertTrue(catalog.recordReminder(id,9,begin,1));assertFalse(catalog.recordReminder(id,9,begin,1));assertFalse(catalog.recordReminder(id,9,begin.plusSeconds(1),1));tx.subscribe(id,10,true);tx.subscribe(id,10,false);assertFalse(catalog.recordReminder(id,10,begin,1));assertEquals(1,jdbc.queryForObject("SELECT COUNT(*) FROM tb_seckill_notification",Integer.class));verifyNoInteractions(delays);}
}
