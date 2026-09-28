package org.javaup.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import jakarta.annotation.Resource;
import org.javaup.dto.Result;
import org.javaup.dto.UserDTO;
import org.javaup.entity.Blog;
import org.javaup.entity.BlogComments;
import org.javaup.entity.User;
import org.javaup.mapper.BlogCommentsMapper;
import org.javaup.mapper.BlogMapper;
import org.javaup.service.IBlogCommentsService;
import org.javaup.service.IUserService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.javaup.utils.SystemConstants;
import org.javaup.utils.UserHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 博客评论接口实现
 * @author: 阿星不是程序员
 **/
@Service
public class BlogCommentsServiceImpl extends ServiceImpl<BlogCommentsMapper, BlogComments> implements IBlogCommentsService {

    @Resource
    private BlogMapper blogMapper;

    @Resource
    private IUserService userService;

    @Resource
    private SnowflakeIdGenerator snowflakeIdGenerator;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result saveComment(BlogComments comment) {
        if (Objects.isNull(comment) || Objects.isNull(comment.getBlogId())) {
            return Result.fail("博客id不能为空");
        }
        if (StrUtil.isBlank(comment.getContent())) {
            return Result.fail("评论内容不能为空");
        }
        Blog blog = blogMapper.selectById(comment.getBlogId());
        if (Objects.isNull(blog)) {
            return Result.fail("笔记不存在！");
        }
        UserDTO user = UserHolder.getUser();
        comment.setId(snowflakeIdGenerator.nextId());
        comment.setUserId(user.getId());
        comment.setParentId(Objects.isNull(comment.getParentId()) ? 0L : comment.getParentId());
        comment.setAnswerId(Objects.isNull(comment.getAnswerId()) ? 0L : comment.getAnswerId());
        comment.setLiked(0);
        comment.setStatus(false);
        comment.setCreateTime(LocalDateTime.now());
        comment.setUpdateTime(LocalDateTime.now());

        boolean saved = save(comment);
        if (!saved) {
            return Result.fail("新增评论失败");
        }
        blogMapper.update(null, new UpdateWrapper<Blog>()
                .setSql("comments = comments + 1")
                .eq("id", comment.getBlogId()));
        comment.setName(user.getNickName());
        comment.setIcon(user.getIcon());
        return Result.ok(comment);
    }
    // 方法功能：保存 comment 相关业务数据。

    @Override
    public Result queryCommentsByBlogId(Long blogId, Integer current) {
        if (Objects.isNull(blogId)) {
            return Result.fail("博客id不能为空");
        }
        Page<BlogComments> page = query()
                .eq("blog_id", blogId)
                .eq("parent_id", 0)
                .eq("status", 0)
                .orderByDesc("create_time")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        List<BlogComments> records = page.getRecords();
        fillCommentUser(records);
        return Result.ok(records);
    }
    // 方法功能：分页查询指定博客下的一级评论。

    @Override
    public Result queryRepliesByParentId(Long parentId, Integer current) {
        if (Objects.isNull(parentId)) {
            return Result.fail("父评论id不能为空");
        }
        Page<BlogComments> page = query()
                .eq("parent_id", parentId)
                .eq("status", 0)
                .orderByAsc("create_time")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        List<BlogComments> records = page.getRecords();
        fillCommentUser(records);
        return Result.ok(records);
    }
    // 方法功能：分页查询指定评论的回复列表。

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result removeComment(Long id) {
        if (Objects.isNull(id)) {
            return Result.fail("评论id不能为空");
        }
        BlogComments comment = getById(id);
        if (Objects.isNull(comment)) {
            return Result.fail("评论不存在");
        }
        Long userId = UserHolder.getUser().getId();
        if (!Objects.equals(comment.getUserId(), userId)) {
            return Result.fail("只能删除自己的评论");
        }
        boolean removed = removeById(id);
        if (!removed) {
            return Result.fail("删除评论失败");
        }
        blogMapper.update(null, new UpdateWrapper<Blog>()
                .setSql("comments = CASE WHEN comments > 0 THEN comments - 1 ELSE 0 END")
                .eq("id", comment.getBlogId()));
        return Result.ok();
    }
    // 方法功能：删除评论并同步更新博客评论数量。

    private void fillCommentUser(List<BlogComments> comments) {
        if (comments == null || comments.isEmpty()) {
            return;
        }
        List<Long> userIds = comments.stream()
                .map(BlogComments::getUserId)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
        if (userIds.isEmpty()) {
            return;
        }
        Map<Long, User> userMap = userService.listByIds(userIds)
                .stream()
                .collect(Collectors.toMap(User::getId, Function.identity(), (left, right) -> left));
        for (BlogComments comment : comments) {
            User user = userMap.get(comment.getUserId());
            if (user != null) {
                comment.setName(user.getNickName());
                comment.setIcon(user.getIcon());
            }
        }
    }
    // 方法功能：为评论列表填充评论用户的展示信息。
}
