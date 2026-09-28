package org.javaup.seckill;

import static org.junit.jupiter.api.Assertions.*;

import org.javaup.dto.UserDTO;
import org.javaup.utils.LoginInterceptor;
import org.javaup.utils.UserHolder;
import org.junit.jupiter.api.*;
import org.springframework.mock.web.*;

class SeckillSecurityTest {
  @AfterEach
  void clear() {
    UserHolder.removeUser();
  }

  @Test
  void unauthenticatedIs401WithStableCode() throws Exception {
    var response = new MockHttpServletResponse();
    assertFalse(
        new LoginInterceptor().preHandle(new MockHttpServletRequest(), response, new Object()));
    assertEquals(401, response.getStatus());
    assertTrue(response.getContentAsString().contains("UNAUTHENTICATED"));
  }

  @Test
  void defaultAdminListDeniesEvenLoggedInUser() {
    var u = new UserDTO();
    u.setId(7L);
    UserHolder.saveUser(u);
    var req = new MockHttpServletRequest("POST", "/voucher/seckill");
    var e =
        assertThrows(
            SeckillFailure.class,
            () ->
                new SeckillSecurityInterceptor("")
                    .preHandle(req, new MockHttpServletResponse(), new Object()));
    assertEquals(403, e.getHttpStatus());
  }

  @Test
  void onlyConfiguredAdminCanWrite() {
    var u = new UserDTO();
    u.setId(7L);
    UserHolder.saveUser(u);
    assertTrue(
        new SeckillSecurityInterceptor("7")
            .preHandle(
                new MockHttpServletRequest("POST", "/voucher/update/seckill/stock"),
                new MockHttpServletResponse(),
                new Object()));
  }
}
