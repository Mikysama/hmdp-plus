package org.javaup.config;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.javaup.dto.Result;
import org.javaup.exception.ArgumentError;
import org.javaup.exception.HmdpFrameException;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 异常处理器
 * @author: 阿星不是程序员
 */
@Slf4j
@RestControllerAdvice
public class WebExceptionAdvice {

  @ExceptionHandler(org.javaup.seckill.SeckillFailure.class)
  public Result<String> seckillFailure(
      org.javaup.seckill.SeckillFailure error, jakarta.servlet.http.HttpServletResponse response) {
    response.setStatus(error.getHttpStatus());
    Result<String> result = Result.fail(error.getCode());
    result.setCode(error.getCode());
    return result;
  }

  /** 业务异常 */
  @ExceptionHandler(value = HmdpFrameException.class)
  public Result<String> toolkitExceptionHandler(
      HttpServletRequest request,
      HmdpFrameException hmdpFrameException,
      jakarta.servlet.http.HttpServletResponse response) {
    log.error(
        "业务异常 错误信息 : {} method : {} url : {} query : {} ",
        hmdpFrameException.getMessage(),
        request.getMethod(),
        getRequestUrl(request),
        getRequestQuery(request),
        hmdpFrameException);
    if (java.util.Objects.equals(hmdpFrameException.getCode(), 10007)
        || java.util.Objects.equals(hmdpFrameException.getCode(), 10008)) response.setStatus(429);
    Result<String> result = Result.fail(hmdpFrameException.getMessage());
    result.setCode(String.valueOf(hmdpFrameException.getCode()));
    return result;
  }

  // 方法功能：处理业务框架异常并返回统一错误响应。
  /** 参数验证异常 */
  @SneakyThrows
  @ExceptionHandler(value = MethodArgumentNotValidException.class)
  public Result<List<ArgumentError>> validExceptionHandler(
      HttpServletRequest request,
      MethodArgumentNotValidException ex,
      jakarta.servlet.http.HttpServletResponse response) {
    response.setStatus(400);
    log.warn("Invalid request method={} path={}", request.getMethod(), request.getRequestURI());
    BindingResult bindingResult = ex.getBindingResult();
    List<ArgumentError> argumentErrorList =
        bindingResult.getFieldErrors().stream()
            .map(
                fieldError -> {
                  ArgumentError argumentError = new ArgumentError();
                  argumentError.setArgumentName(fieldError.getField());
                  argumentError.setMessage(fieldError.getDefaultMessage());
                  return argumentError;
                })
            .collect(Collectors.toList());
    Result<List<ArgumentError>> result = Result.fail(argumentErrorList);
    result.setCode("INVALID_ARGUMENT");
    return result;
  }

  // 方法功能：处理参数校验异常并返回字段级错误信息。

  @ExceptionHandler({
    org.springframework.http.converter.HttpMessageNotReadableException.class,
    org.springframework.web.method.annotation.MethodArgumentTypeMismatchException.class,
    org.springframework.web.bind.MissingServletRequestParameterException.class
  })
  public Result<String> malformedInput(
      Exception error, jakarta.servlet.http.HttpServletResponse response) {
    response.setStatus(400);
    Result<String> result = Result.fail("请求参数格式错误");
    result.setCode("INVALID_ARGUMENT");
    return result;
  }

  /** 拦截未捕获异常 */
  @ExceptionHandler(value = Throwable.class)
  public Result<String> defaultErrorHandler(
      HttpServletRequest request,
      Throwable throwable,
      jakarta.servlet.http.HttpServletResponse response) {
    response.setStatus(request.getRequestURI().startsWith("/voucher") ? 503 : 500);
    log.error(
        "全局异常 错误信息 : {} method : {} url : {} query : {} ",
        throwable.getMessage(),
        request.getMethod(),
        getRequestUrl(request),
        getRequestQuery(request),
        throwable);
    Result<String> result = Result.fail();
    result.setCode("DEPENDENCY_UNAVAILABLE");
    return result;
  }

  // 方法功能：兜底处理未捕获异常并返回统一失败响应。

  private String getRequestUrl(HttpServletRequest request) {
    return request.getRequestURL().toString();
  }

  // 方法功能：拼接当前请求的 URL 和查询参数用于日志输出。

  private String getRequestQuery(HttpServletRequest request) {
    return "[redacted]";
  }
  // 方法功能：读取请求查询字符串，空值时返回占位符。
}
