# Dashboard

React 控制台复用页面设计，读取当前进程的真实会话、订阅、保留消息元数据、
MQTT PUBLISH/字节计数和启动配置。管理接口均为只读，与 MQTT 身份认证相互独立。

## 构建和运行

```sh
cd dashboard/web
npm ci
npm run build
cd ../..
cargo build --release -p rmqtt
# 创建权限受限的令牌文件；不要将它加入 Git 或输出到日志。
(umask 077; openssl rand -hex 32 > /tmp/rmqtt-admin.token)
target/release/rmqtt --listen 127.0.0.1:1883 \
  --admin-listen 127.0.0.1:8080 --admin-token-file /tmp/rmqtt-admin.token \
  --dashboard-dir dashboard/web/dist
```

打开 `http://127.0.0.1:8080/`，在登录页输入令牌文件内容。令牌只留在页面内存，
刷新后需重新输入。退出时清除页面数据。默认未设置管理 CLI 时不启用管理端口。
服务器远程访问使用 SSH 本地转发，例如 `ssh -L 8080:127.0.0.1:8080 USER@HOST`。
该 HTTP 服务仅允许 loopback 数字地址；TLS/反向代理与访问控制由部署环境配置。

服务端启动时加载静态文件，没有 Node 生产运行时依赖。默认目录缺失时 API
仍可启动并输出明确提示；显式 `--dashboard-dir` 错误时启动失败。只更新静态
文件也需重启管理服务，本阶段未实现热加载。

## 指标语义与范围

- 在线/离线会话、排队、inflight、订阅和保留消息读取已提交状态，跨会话视图
  为最终一致。持久数据恢复后可见；不创建额外持久化或读取未提交 SQLite 状态。
- 消息计数为入站收到/出站成功 flush 的 PUBLISH 帧数，包含重传；字节为编码
  MQTT 帧，排除 TCP/TLS/WS 开销。它们不是业务唯一消息数或事务成功数。
- 速率由页面两次成功采样的计数差与经过时间计算。进程重启清空历史；历史
  保存在当前页面内存，最多 60 个样本，不是长时监控系统。
- CPU/RSS、事件历史、规则引擎和管理变更尚未实现；页面标记不可用，不显示
  演示指标。没有证书身份映射，不推断客户端 IP/用户名。
- 不暴露消息 Payload、令牌、密码哈希、证书、私钥及配置文件路径。

精确接口与资源限额见 [Phase4f 契约](contracts/phase4f-dashboard.md)。

## 前端组件

React + Tailwind CSS v4 + shadcn/ui。布局使用工具类，按钮、卡片、输入框、
徽章、表格和原生选择框使用官方组件源码，图标使用 Lucide。只保留61行主题、
基础样式及原生 dialog 背景 CSS。组件来源与许可见
`dashboard/web/THIRD_PARTY_NOTICES.md`；详情面板使用原生 dialog，保持严格 CSP。
