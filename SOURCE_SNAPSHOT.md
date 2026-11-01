# 定制版源码来源与合并记录

## 当前维护基准（0.2.19）

自 2026-11-01 起，本私有定制版的唯一维护上游为 [Wei-Shaw/sub2api](https://github.com/Wei-Shaw/sub2api)，本轮公开源码对照固定为 `d851a38477597157e78450d30af2d8504764d085`。`origin` 保持 `Moeblack/sub2api`，`upstream` 指向 `https://github.com/Wei-Shaw/sub2api.git`；luren 分支只保留为历史来源，不再作为后续同步基准。

本轮从私有 `90e76dff93e595271290fdcaaa8742863dd45ced`（0.2.18）选择性回移公开修复和兼容补丁，不用上游整树覆盖私有源码，也不宣称已经完整合并 Wei-Shaw main。Codex 原生 WebSocket/ctx_pool、输入及工具语义、GPT Image 2 系列流式生成与编辑、自定义模型 ID 直通、已有安全加固及其它私有功能继续保留。

0.2.19 的范围包括供应商凭据隔离、CC-only 降级与错误分类、废弃配置说明、协议 base URL、显式能力与安全字段保真、缓存 TTL/计费明细、模型级错误处理、输出预算保护、媒体重试保真、缓存失效并发保护和流超时/辅助请求边界。不逆向 CLI，不恢复 CCH 签名，不补造 TLS 或未公开客户端特征。构建、验证、生产切换与回滚的最终结果以本次发布和运维记录为准。

## 历史集成版本（0.2.4）

`0.2.4` 在 luren 定制预发布 `b4c6960b92aacdc30c98b350f218873b6928030d` 基础上合入 Wei-Shaw `v0.2.4` 对应提交 `5de5e2bed035d43591a2e10e51f420ef6a84eb98`（共同祖先 `270eac6973049fe1b50eb75560a74a029e82884c`）。保留原有输入语义、工具 ID、连接隔离、原生 WebSocket、Steering 与异步工具定制，加入 Image 2.5 模型目录、计费与 OAuth 图片主控修复；本仓库的版本文件显式标为 `0.2.4`，不代表采用了官方二进制。

本轮合并保留公开历史；只进行了 Git 合并与静态差异核对，未编译、未运行测试、未部署或使用真实账号验收。此前 luren 集成的范围与验证记录见[历史升级记录](docs/upgrades/2026-09-08-openai.md)，这些历史结果不等于本次合并已通过相同验证。实际客户端与目标账号能力仍需另行确认。

## 初始快照

本仓库保存 2026-09-08 整理的 Sub2API 定制版源码，包含当时工作目录中尚未提交的后端修改及相关测试。

- 上游项目：<https://github.com/Wei-Shaw/sub2api>
- 上游基准提交：`578785ee7`。
- 初始程序版本：`0.2.1`。
- 源码来源：本机 `0.2.1-input-semantics-753865520606` 部署所使用的工作目录。
- 初次上传采用独立的源码快照提交，未导入原工作目录的 Git 历史。

保留后端、前端、数据库迁移、资源文件、测试、构建配置、部署示例、项目文档及原有许可证。未包含生产配置、数据库、运行日志、编译产物、本机部署/探测脚本，以及 `openspec` 中另一项目的 `source-freeze` 归档。

初次上传的是源码快照。后续提供了与本机部署程序一致的 [Linux x86_64 历史编译包](https://github.com/lurenxing628/sub2api/releases/tag/v0.2.1-input-semantics-753865520606)，包含程序、网页界面、资源、启动脚本和使用说明；PostgreSQL、Redis 需另行准备。此链接仅记录历史来源，当前定制发布物应使用 [Moeblack/sub2api Releases](https://github.com/Moeblack/sub2api/releases)。

本仓库的更新检查、Release 下载与安装脚本来源为 `Moeblack/sub2api`；没有适配本定制版的 Release 时不要把其他上游发布物当作替代。生产部署仅使用本 fork 固定提交构建、带版本及 revision 标签的镜像，并单独保留旧程序和回滚镜像。历史部署示例或界面中的 `weishaw/sub2api` / `latest` 命令不能作为定制版回滚依据。

构建及配置说明见 [开发指南](DEV_GUIDE.md)、[中文说明](README_CN.md) 和 [部署文档](deploy/README.md)。
