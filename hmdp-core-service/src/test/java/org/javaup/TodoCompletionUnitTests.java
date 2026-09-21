package org.javaup;

import org.javaup.dto.LoginFormDTO;
import org.javaup.dto.Result;
import org.javaup.service.impl.ShopServiceImpl;
import org.javaup.service.impl.UserServiceImpl;
import org.javaup.utils.PasswordEncoder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TodoCompletionUnitTests {

    @Test
    void bcryptPasswordCanBeVerified() {
        String encoded = PasswordEncoder.encode("password123");

        assertNotEquals("password123", encoded);
        assertTrue(PasswordEncoder.matches(encoded, "password123"));
        assertFalse(PasswordEncoder.matches(encoded, "wrong-password"));
    }

    @Test
    void loginRejectsMissingOrConflictingCredentials() {
        UserServiceImpl service = new UserServiceImpl();
        LoginFormDTO missing = new LoginFormDTO();
        missing.setPhone("13800138000");

        Result<String> missingResult = service.login(missing, null);
        assertFalse(missingResult.getSuccess());

        LoginFormDTO conflicting = new LoginFormDTO();
        conflicting.setPhone("13800138000");
        conflicting.setCode("123456");
        conflicting.setPassword("password123");

        Result<String> conflictingResult = service.login(conflicting, null);
        assertFalse(conflictingResult.getSuccess());
    }

    @Test
    void geoQueryRejectsOnlyOneCoordinate() {
        ShopServiceImpl service = new ShopServiceImpl();

        Result result = service.queryShopByType(1, 1, 120.1, null);

        assertFalse(result.getSuccess());
    }
}
