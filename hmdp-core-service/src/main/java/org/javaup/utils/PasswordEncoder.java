package org.javaup.utils;


import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 密码
 * @author: 阿星不是程序员
 **/
public class PasswordEncoder {

    private static final BCryptPasswordEncoder ENCODER = new BCryptPasswordEncoder();

    public static String encode(String password) {
        return ENCODER.encode(password);
    }

    public static Boolean matches(String encodedPassword, String rawPassword) {
        if (encodedPassword == null || encodedPassword.isBlank() || rawPassword == null) {
            return false;
        }
        return ENCODER.matches(rawPassword, encodedPassword);
    }
}
