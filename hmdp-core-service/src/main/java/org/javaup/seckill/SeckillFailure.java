package org.javaup.seckill;

public class SeckillFailure extends RuntimeException {
  private final String code;
  private final int httpStatus;

  public SeckillFailure(String code, int status) {
    super(code);
    this.code = code;
    this.httpStatus = status;
  }

  public String getCode() {
    return code;
  }

  public int getHttpStatus() {
    return httpStatus;
  }
}
