# 单机运行说明

本项目运行在本机的 Next.js 服务上，不需要 Cloudflare 账号、网络授权或外网部署。页面中的课程切换、进度记录和音频播放等交互会正常工作。

## 运行要求

- Windows 电脑
- Node.js 20 或以上（建议使用当前 LTS 版本）

## 使用预构建单机包（推荐）

1. 解压 `english-animation-course-local-20260731.zip`，进入其中的 `english-animation-course-local-20260731` 目录。
2. 在该目录打开 PowerShell，执行：

   ```powershell
   $env:HOSTNAME = "127.0.0.1"
   $env:PORT = "3005"
   node .\server.js
   ```

3. 浏览器打开 <http://127.0.0.1:3005>。
4. 关闭 PowerShell 窗口即可停止服务。

预构建包已包含网站运行所需的 JavaScript 和课程资源；本机只需提供 Node.js 运行时，不需要执行 `npm install`。

## 从源码运行

```powershell
npm ci
npm run build:local
$env:HOSTNAME = "127.0.0.1"
$env:PORT = "3005"
npm run start:local
```

## 更新内容后

修改课程、音频或页面代码后，重新执行 `npm run build:local`，然后按“使用预构建单机包”的目录结构重新打包。
