## 文档目标
本文档用于规范项目在 GitHub 上的多人协作流程，覆盖分支创建、代码提交、PR创建、CI门禁、代码评审、线性历史维护以及合入后的分支清理。所有协作者在参与项目开发前，均应阅读并遵守本文档，以保证主分支稳定、提交历史清晰、代码变更可追踪、工程流程可复现。

> **PR**  
PR 是 Pull Request 的缩写，表示开发者将自己分支上的代码变更请求合入目标分支的协作机制。项目中所有非紧急变更都应通过 PR 完成，不允许直接向 main 分支提交代码。

> **CI**  
CI 是 Continuous Integration 的缩写，表示在代码提交或 PR 创建后，由 GitHub Actions 自动执行编译、测试、格式检查等流程。CI 通过后，代码才具备进入主分支的基本条件。

## 分支规范

### 主分支
项目统一使用 `main` 作为主分支。`main` 分支应始终保持可编译、可运行、可测试的稳定状态。任何协作者不得直接向 `main` 分支提交代码，所有功能开发、问题修复、文档更新和工程配置修改都必须通过独立分支完成，并通过 PR 请求合入。

目前仓库 `main` 分支配置了 Branch Protection Rules，**禁止代码推送**。

### 开发分支
工作分支是协作者进行具体开发的分支。每个工作分支应只对应一个明确任务，不应在同一个分支中同时混入多个无关功能。例如，成员管理页面开发、配置修复和文档更新应分别使用不同分支，而不是集中在一个分支中完成。

工作分支应从最新的 `main` 分支创建。创建分支前，协作者必须先拉取远程最新状态，避免基于过期代码开展开发。

```bash
git switch main
git pull origin main
git switch -c feature/pz-module-description
```
上述命令首先切换到 `main` 分支，然后从远程仓库拉取最新代码，最后创建新的功能分支 `feature/pz-module-description`。所有新任务都应遵循该流程创建分支，避免直接在已有旧分支上继续叠加无关改动。

**分支命名规范**

分支名统一采用如下格式。
```text
<type>/<owner>-<module>-<description>
```
其中 `type` 表示任务类型，`owner` 表示分支负责人，`module` 表示变更所属模块，`description` 表示简短任务说明。分支名全部使用小写英文字母、数字和连字符，不允许使用中文、空格、下划线和大写字母。

常用的 `type` 类型有：
| `type` | 含义 | 适用场景 | 分支示例 |
| --- | --- | --- | --- |
| `feature` | 新功能开发 | 新增用户管理、权限控制、任务面板、接口能力、页面模块等功能 | `feature/pz-member-management` |
| `fix` | 问题修复 | 修复界面错误、逻辑缺陷、接口异常、数据状态错误、兼容性问题等 | `fix/yjy-task-card-style` |
| `refactor` | 代码重构 | 在不改变外部行为的前提下，调整代码结构、拆分模块、优化命名、消除重复逻辑 | `refactor/pz-project-permission` |
| `docs` | 文档修改 | 修改 README、部署说明、接口文档、协作规范、使用说明等 | `docs/dsf-collaboration-guide` |
| `test` | 测试相关修改 | 新增或修改单元测试、集成测试、测试样例、测试工具、测试数据等 | `test/wjh-user-service-test` |
| `ci` | 持续集成与工程门禁 | 修改 GitHub Actions、Rulesets、构建脚本、自动化检查、发布流程等 | `ci/zyb-github-actions` |
| `chore` | 工程杂项维护 | 依赖升级、配置调整、目录整理、脚手架维护、无业务逻辑变化的工程维护 | `chore/wjh-update-dependencies` |
| `hotfix` | 紧急修复 | 主分支或线上环境出现严重问题时的快速修复 | `hotfix/dsf-login-crash` |

例如：
```text
feature/pz-member-management
fix/yjy-task-card-style
ci/zyb-github-actions
docs/wjh-readme-update
test/dsf-user-service-test
```
上述分支名能够同时表达任务类型、负责人、影响模块和变更目的，便于协作者在 GitHub 分支列表、PR 列表和 CI 记录中快速识别任务来源。推荐使用该命名方式，目前仓库**没有**显式做分支命名约束。

## 提交规范
每次提交应对应一个清晰、完整、可解释的逻辑变更。提交内容不应同时包含多个无关主题。例如，修复按钮样式和新增成员权限接口应拆分为两个提交或两个 PR。提交前应确认代码能够编译通过，基础功能能够运行，且不包含临时调试代码、无意义日志、未使用文件和本地环境配置。

项目要求 `main` 分支保持**线性历史**。线性历史意味着主分支提交记录是一条清晰的直线，不包含普通 merge commit。该策略可以降低历史阅读成本，使回滚、问题定位和版本审计更加简单。

> **如何保证线性历史？**   
> 协作者在本地开发过程中，应避免通过普通 `merge` 将 `main` 合入工作分支。普通 `merge` 容易在工作分支中产生 merge commit，后续如果该分支被合入 `main`，就可能破坏主分支的线性历史。项目统一要求使用 `rebase` 将工作分支更新到最新 `main` 之后。
> 当远程 `main` 分支已经更新，而当前工作分支需要同步这些更新时，应使用如下命令。
> ```bash
> git fetch origin
> git rebase origin/main
> ```
> 上述命令会先获取远程最新状态，然后切换到当前工作分支，最后将当前分支上的本地提交重新排列到最新 `origin/main` 之后。这样可以避免产生额外的 merge commit，使工作分支历史保持为一条直线。
> 协作者**不应使用**如下命令更新工作分支。
> ```bash
> git merge origin/main
> ```
> 如果 `rebase` 过程中出现冲突，应手动解决冲突文件，然后继续执行 `rebase`。
> ```bash
> git status
> git add -A
> git rebase --continue
> ```

如果开发过程中产生了多个临时提交，例如 debug、fix again、update、try ci，在提交 PR 前应将这些提交整理为一个或少量语义清晰的提交。项目**推荐一个 PR 最终只保留一个提交记录**，以保证主分支历史简洁。

> **如何只保留一个提交记录？**   
> 假设当前工作分支相对 `main` 已经产生 `A`、`B`、`C` 三个新提交，并且工作区还有新的未提交修改，最终目标是将 `A`、`B`、`C` 和未提交修改统一整理为一个提交。    
> 先更新远程主分支信息，并确认当前分支基于最新 `main`。
> ```bash
> git fetch origin
> ```
> 如果当前分支已经落后于 `origin/main`，应先执行 `rebase`，将当前分支变基到最新主分支之后。   
> ```bash
> git rebase origin/main
> ```
> 如果当前工作区存在未提交修改，`rebase` 可能会被拒绝。此时应先临时保存工作区修改，再执行 `rebase`，最后恢复修改。   
> ```bash
> git stash
> git rebase origin/main
> git stash pop
> ```
> 完成上述步骤后，即可使用 `git reset --soft` 将当前分支相对 `origin/main` 的所有提交回退为暂存区中的代码变更。
> ```bash
> git reset --soft origin/main
> ```
> `git reset --soft origin/main` 会将当前分支指针移动到 `origin/main`，但不会丢弃原先 `A`、`B`、`C` 中包含的代码修改，也不会丢弃工作区中尚未提交的修改。执行后，原有多个提交会被展开为当前分支上的待提交变更。
> 随后将所有变更加入暂存区，并重新创建一个清晰的提交。
> ```bash
> git add -A
> git commit -m "feat: add project member management"
> ```
> 提交完成后，应检查当前分支相对 `origin/main` 是否只包含一个提交。
> ```bash
> git log
> ```

提交信息采用如下格式。
```text
<type>: <summary>
```

其中 type 与分支类型保持一致，summary 使用简短英文描述本次变更。示例如下。

```text
feat: add project member management
fix: correct task card collapse style
ci: add github actions workflow
docs: add github collaboration guide
refactor: simplify project permission model
```

提交信息应避免使用 update、fix bug、change code、final 等含义模糊的描述。提交信息应能够让审查者在不打开代码的情况下，基本判断本次提交解决了什么问题。

## 推送规范
协作者完成本地提交后，应将当前工作分支推送到远程同名分支。远程分支名**必须与本地分支名保持一致**，不允许将本地分支推送到另一个不同名称的远程分支。该约束可以避免 PR 来源分支混乱，也便于审查者根据分支名识别任务类型、负责人和变更模块。

例如，本地分支为 `feature/peng-member-management` 时，远程分支也必须命名为 `feature/peng-member-management`。不允许将该分支推送为 `feature/member`、`test`、`dev` 或其他临时名称。

首次推送当前分支时，推荐使用如下命令。

```bash
git push -u origin HEAD
```

该命令会将当前本地分支推送到远程 `origin` 中的同名分支，并建立本地分支与远程同名分支之间的 upstream 关系。建立 upstream 后，后续在该分支上继续提交修改时，可以直接使用 `git push` 推送更新。

也可以显式写出本地分支名。

```bash
git push -u origin feature/pz-member-management
```

## PR 规范
### 提交 PR
代码推送成功后，终端通常会输出一个用于创建 PR 的 GitHub 页面地址。该地址一般位于推送结果的末尾，形式类似如下内容。
```text
remote: Create a pull request for 'feature/peng-member-management' on GitHub by visiting:
remote:      https://github.com/<owner>/<repo>/pull/new/feature/pz-member-management
```
协作者可以直接复制该地址并在浏览器中打开。打开后会进入 GitHub 的 PR 创建页面。若终端未显示该地址，也可以进入 GitHub 仓库首页，通常页面顶部会出现当前新推送分支的提示区域，并显示 `Compare & pull request` 按钮。点击该按钮同样可以进入 PR 创建页面。


进入 PR 创建页面后，应确认目标分支和源分支设置正确。目标分支应选择 `main`，源分支应选择当前工作分支。例如，当前开发分支为 `feature/peng-member-management` 时，PR 页面中应显示当前分支请求合入 `main`。

PR 标题应使用清晰、简短、可审查的描述，推荐与最终提交信息保持一致。
PR 描述应说明本次变更的背景、主要修改内容、测试情况和潜在影响。

确认目标分支、源分支、标题和描述均正确后，点击 GitHub 页面中的 `Create pull request` 按钮创建 PR。

PR 创建完成后，GitHub 会进入该 PR 的详情页面。页面中会显示代码差异、提交记录、评论区域、审查状态和 CI 检查状态。协作者应首先检查 `Files changed` 页面，确认本次 PR 只包含当前任务相关改动，不包含本地临时文件、调试日志、编译产物、敏感配置和无关格式化修改。

### CI 检查
项目配置了 CI 门禁检查。

PR 创建后，GitHub Actions 会自动触发 CI 检查。项目中的 CI 会执行编译、单元测试、格式检查。

当 CI 正在运行时，协作者应等待检查完成。当 CI 通过时，PR 页面会显示绿色通过状态。**只有所有必需检查通过**后，PR 才允许合入 `main`。

若 CI 失败，协作者不得绕过检查直接请求合入。应点击失败的检查项进入 GitHub Actions 日志页面，查看具体失败原因。修复问题后，在当前工作分支继续提交修改并推送到远程同名分支，PR 会自动更新并重新触发 CI。

### Code Review
CI 通过后，PR 仍然需要经过代码审查。协作者应在 PR 页面中指定审查者，或按照项目约定通知相关模块负责人进行 Review。审查者可能会在代码行、文件或整体 PR 下提出修改意见。

协作者收到审查意见后，应在原工作分支继续修改，不应关闭当前 PR 后重新创建新 PR。修改完成后继续推送到远程同名分支，GitHub 会自动更新当前 PR。

所有关键评论都应在 PR 页面中处理。**所有评论线程必须被标记为 resolved **后，PR 才允许合入。

### PR 合入
当 PR 同时满足 CI 通过、代码审查通过、评论已解决、分支基于最新 `main` 和提交历史符合规范后，才允许合入。项目默认使用 `Squash and merge` 合入方式。

所有面向 main 的 PR 至少需要一名协作者审核通过后才能合入。审查者应重点关注代码正确性、边界条件、错误处理、权限逻辑、接口兼容性、可维护性和测试覆盖情况。对于涉及权限、数据删除、部署脚本、CI 配置和安全策略的 PR，应由项目负责人或仓库管理员进行额外确认。

被审查者应及时回应评论。若审查者提出修改意见，应在本地修改后继续推送到原分支，不需要重新创建 PR。

使用 `Squash and merge` 时，GitHub 会将当前 PR 中的所有提交压缩为一个提交后合入 `main`。合入前应检查最终提交标题，确保其符合提交规范。

PR 合入后，应删除远程工作分支。若 GitHub 页面显示 `Delete branch` 按钮，协作者可以直接点击删除远程分支。

在确认 PR 已经成功合入 `main` 后，可以使用强制删除本地分支。

```bash
git branch -D feature/pz-member-management
```

完成清理后，下一项任务应重新从最新 `main` 创建新的工作分支，不应继续在已经合入的旧分支上开发。





