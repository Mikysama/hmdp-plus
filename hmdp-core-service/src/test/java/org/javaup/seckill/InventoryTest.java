package org.javaup.seckill;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class InventoryTest {
  @Test
  void reserveThenCommitPreservesQuota() {
    var i = new Inventory(100, 100, 0, 0).reserve().commit();
    assertEquals(new Inventory(100, 99, 0, 1), i);
  }

  @Test
  void reserveThenReleasePreservesQuota() {
    assertEquals(new Inventory(1, 1, 0, 0), new Inventory(1, 1, 0, 0).reserve().release());
  }

  @Test
  void cannotCancelMoreThanSold() {
    assertThrows(IllegalStateException.class, () -> new Inventory(1, 1, 0, 0).cancel());
  }

  @Test
  void cannotShrinkBelowAllocated() {
    assertThrows(IllegalStateException.class, () -> new Inventory(10, 2, 3, 5).adjust(7));
  }

  @Test
  void cannotReserveSoldOut() {
    assertThrows(IllegalStateException.class, () -> new Inventory(1, 0, 0, 1).reserve());
  }

  @Test
  void corruptInventoryFailsClosed() {
    assertThrows(IllegalStateException.class, () -> new Inventory(10, 8, 1, 0));
  }
}
