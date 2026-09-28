package org.javaup.controller;


import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import jakarta.annotation.Resource;
import org.javaup.dto.Result;
import org.javaup.dto.UserDTO;
import org.javaup.entity.Blog;
import org.javaup.service.IBlogService;
import org.javaup.utils.SystemConstants;
import org.javaup.utils.UserHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 博客api
 * @author: 阿星不是程序员
 **/
@RestController
@RequestMapping("/blog")
public class BlogController {

    @Resource
    private IBlogService blogService;

    @PostMapping
    public Result saveBlog(@RequestBody Blog blog) {
        return blogService.saveBlog(blog);
    }
    // 方法功能：保存博客并推送到粉丝收件箱。

    @PutMapping("/like/{id}")
    public Result likeBlog(@PathVariable("id") Long id) {
        return blogService.likeBlog(id);
    }
    // 方法功能：切换当前用户对博客的点赞状态并同步点赞计数。

    @GetMapping("/of/me")
    public Result queryMyBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .eq("user_id", user.getId()).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }
    // 方法功能：查询 my blog 相关数据并返回结果。

    @GetMapping("/hot")
    public Result queryHotBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        return blogService.queryHotBlog(current);
    }
    // 方法功能：分页查询热门博客并补充用户和点赞状态。

    @GetMapping("/{id}")
    public Result queryBlogById(@PathVariable("id") Long id) {
        return blogService.queryBlogById(id);
    }
    // 方法功能：按 ID 查询博客详情并补充作者与点赞状态。

    @GetMapping("/likes/{id}")
    public Result queryBlogLikes(@PathVariable("id") Long id) {
        return blogService.queryBlogLikes(id);
    }
    // 方法功能：查询指定博客最近点赞用户列表。

    @GetMapping("/of/user")
    public Result queryBlogByUserId(
            @RequestParam(value = "current", defaultValue = "1") Integer current,
            @RequestParam("id") Long id) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .eq("user_id", id).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }
    // 方法功能：查询 blog by user id 相关数据并返回结果。

    @GetMapping("/of/follow")
    public Result queryBlogOfFollow(
            @RequestParam("lastId") Long max, @RequestParam(value = "offset", defaultValue = "0") Integer offset){
        return blogService.queryBlogOfFollow(max, offset);
    }
    // 方法功能：滚动分页查询关注用户发布的博客动态。
}
