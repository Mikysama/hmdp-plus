package org.javaup.controller;


import jakarta.annotation.Resource;
import org.javaup.dto.Result;
import org.javaup.entity.BlogComments;
import org.javaup.service.IBlogCommentsService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 博客评论api
 * @author: 阿星不是程序员
 **/
@RestController
@RequestMapping("/blog-comments")
public class BlogCommentsController {

    @Resource
    private IBlogCommentsService blogCommentsService;

    @PostMapping
    public Result saveComment(@RequestBody BlogComments comment) {
        return blogCommentsService.saveComment(comment);
    }
    // 方法功能：保存 comment 相关业务数据。

    @GetMapping("/of/blog/{blogId}")
    public Result queryCommentsByBlogId(@PathVariable("blogId") Long blogId,
                                        @RequestParam(value = "current", defaultValue = "1") Integer current) {
        return blogCommentsService.queryCommentsByBlogId(blogId, current);
    }
    // 方法功能：分页查询指定博客下的一级评论。

    @GetMapping("/of/parent/{parentId}")
    public Result queryRepliesByParentId(@PathVariable("parentId") Long parentId,
                                         @RequestParam(value = "current", defaultValue = "1") Integer current) {
        return blogCommentsService.queryRepliesByParentId(parentId, current);
    }
    // 方法功能：分页查询指定评论的回复列表。

    @DeleteMapping("/{id}")
    public Result removeComment(@PathVariable("id") Long id) {
        return blogCommentsService.removeComment(id);
    }
    // 方法功能：删除评论并同步更新博客评论数量。
}
