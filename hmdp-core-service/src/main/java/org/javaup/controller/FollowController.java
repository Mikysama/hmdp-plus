package org.javaup.controller;


import jakarta.annotation.Resource;
import org.javaup.dto.Result;
import org.javaup.service.IFollowService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 点赞api
 * @author: 阿星不是程序员
 **/
@RestController
@RequestMapping("/follow")
public class FollowController {

    @Resource
    private IFollowService followService;

    @PutMapping("/{id}/{isFollow}")
    public Result follow(@PathVariable("id") Long followUserId, @PathVariable("isFollow") Boolean isFollow) {
        return followService.follow(followUserId, isFollow);
    }
    // 方法功能：关注或取关目标用户并维护 Redis 关注集合。

    @GetMapping("/or/not/{id}")
    public Result isFollow(@PathVariable("id") Long followUserId) {
        return followService.isFollow(followUserId);
    }
    // 方法功能：判断当前用户是否关注目标用户。

    @GetMapping("/common/{id}")
    public Result followCommons(@PathVariable("id") Long id){
        return followService.followCommons(id);
    }
    // 方法功能：查询当前用户与目标用户的共同关注列表。
}
