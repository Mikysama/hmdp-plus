package org.javaup.seckill;

import static org.javaup.constant.Constant.BLOOM_FILTER_HANDLER_VOUCHER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.javaup.handler.BloomFilterHandler;
import org.javaup.handler.BloomFilterHandlerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SeckillVoucherBloomTest {
  final BloomFilterHandler filter = mock(BloomFilterHandler.class);
  final BloomFilterHandlerFactory factory = mock(BloomFilterHandlerFactory.class);
  final SeckillStore store = mock(SeckillStore.class);
  SeckillVoucherBloom bloom;

  @BeforeEach void setup() {
    when(factory.get(BLOOM_FILTER_HANDLER_VOUCHER)).thenReturn(filter);
    when(filter.loadedGeneration()).thenReturn("READY:g1");
    bloom = new SeckillVoucherBloom(factory, store, new SimpleMeterRegistry());
  }

  @Test void completeIndexNegativeRejectsBeforeDatabaseAccess() {
    assertEquals("VOUCHER_NOT_FOUND", assertThrows(SeckillFailure.class, () -> bloom.check(999)).getCode());
    verifyNoInteractions(store);
  }

  @Test void positiveIsOnlyAHintAndDoesNotReadDatabaseInsideGuard() {
    when(filter.contains("1")).thenReturn(true);
    assertDoesNotThrow(() -> bloom.check(1));
    verifyNoInteractions(store);
  }

  @Test void uninitializedIndexDoesNotRejectUncachedDatabaseVoucher() {
    when(filter.loadedGeneration()).thenReturn(null);
    assertDoesNotThrow(() -> bloom.check(1));
    verify(filter, never()).contains(anyString());
  }

  @Test void unavailableFilterFallsBackToDatabasePath() {
    when(filter.loadedGeneration()).thenThrow(new IllegalStateException("Redis unavailable"));
    assertDoesNotThrow(() -> bloom.check(1));
  }

  @Test void changedGenerationDuringNegativeCheckCannotReject() {
    when(filter.loadedGeneration()).thenReturn("READY:g1", "READY:g2");
    assertDoesNotThrow(() -> bloom.check(1));
  }

  @Test void initializationPagesDatabaseIdsBeforeMarkingComplete() {
    when(filter.beginLoad()).thenReturn("g1");
    when(store.catalogVoucherIds(0, 1000)).thenReturn(List.of(1L, 3L));
    when(store.catalogVoucherIds(3, 1000)).thenReturn(List.of());
    when(filter.completeLoad("g1")).thenReturn(true);
    assertTrue(bloom.initialize());
    var ordered = inOrder(filter, store);
    ordered.verify(filter).beginLoad();
    ordered.verify(store).catalogVoucherIds(0, 1000);
    ordered.verify(filter).ensureInitializedAndAdd("1");
    ordered.verify(filter).ensureInitializedAndAdd("3");
    ordered.verify(store).catalogVoucherIds(3, 1000);
    ordered.verify(filter).completeLoad("g1");
  }

  @Test void failedDatabaseScanCannotMarkIndexComplete() {
    when(filter.beginLoad()).thenReturn("g1");
    when(store.catalogVoucherIds(0, 1000)).thenThrow(new IllegalStateException("SQL unavailable"));
    assertFalse(bloom.initialize());
    verify(filter, never()).completeLoad(anyString());
  }

  @Test void lostGenerationCannotBePublishedAsComplete() {
    when(filter.beginLoad()).thenReturn("g1");
    when(store.catalogVoucherIds(0, 1000)).thenReturn(List.of());
    when(filter.completeLoad("g1")).thenReturn(false);
    assertFalse(bloom.initialize());
  }

  @Test void registrationFailurePreventsCommitOfUnindexedVoucher() {
    doThrow(new IllegalStateException("down")).when(filter).ensureInitializedAndAdd("1");
    assertEquals("BLOOM_UNAVAILABLE", assertThrows(SeckillFailure.class, () -> bloom.register(1)).getCode());
  }
}
