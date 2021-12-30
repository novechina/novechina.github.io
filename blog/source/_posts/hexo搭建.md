---
title: hexo博客编辑
date: 2021-12-30 16:58:24
tags: 博客
categories: 兴趣
---
* * 绑定git

  * 绑定本地密匙到git
  * 初始化一个仓库，Settings->Pages->Source：branch设置为master，用来发布页面
  * main分支用来存放文件用作后续编辑

* 安装node.js

  * 官网下载符合操作系统的新版本

* 安装hexo

  ~~~shell
  npm install -g hexo//安装
  hexo init//初始化项目
  ~~~

* 配置主题

  * git搜索，下载解压到**\themes**中
  * **_config.yml**中themes主题名字修改为解压后的名字

* 发布博客

  ~~~shell
  hexo clean
  hexo g
  hexo s//本地部署预览
  hexo d//发布异常，删除目录下的.开头文件夹
  ~~~

* 上传文件到git，方便后续的编辑

  ~~~
  git add .
  git commit -m "修改备注"
  git push
  ~~~

  

