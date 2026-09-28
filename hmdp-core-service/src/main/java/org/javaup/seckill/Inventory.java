package org.javaup.seckill;

/** Immutable inventory arithmetic. Every transition enforces quota conservation. */
public record Inventory(int total, int available, int reserved, int sold) {
  public Inventory {
    if (total < 0
        || available < 0
        || reserved < 0
        || sold < 0
        || (long) available + reserved + sold != total) {
      throw new IllegalStateException("INVENTORY_INVARIANT");
    }
  }

  public Inventory reserve() {
    return new Inventory(total, available - 1, reserved + 1, sold);
  }

  public Inventory commit() {
    return new Inventory(total, available, reserved - 1, sold + 1);
  }

  public Inventory release() {
    return new Inventory(total, available + 1, reserved - 1, sold);
  }

  public Inventory cancel() {
    return new Inventory(total, available + 1, reserved, sold - 1);
  }

  public Inventory adjust(int newTotal) {
    return new Inventory(
        newTotal, Math.toIntExact((long) available + newTotal - total), reserved, sold);
  }
}
