# Knative Auto-Conversion 设计草案

> 状态：Draft / 需评审
> 关联 Operator：`if-operator`（group `cache.ifbiu.com`，账本 CRD `KnativeAdoption`）
> 目标读者：本仓库维护者、Knative 平台同学

---

## 1. 目标与非目标

### 1.1 目标
- 用户**无需改动自己的应用 YAML**，只要在 `Deployment`（或其它 workload）上打上指定 labels / annotations，平台就自动把它「托管」到 Knative，获得：
  - 无请求时缩容到 0；
  - 有请求/事件到来时自动拉起；
  - 可选的事件驱动入口（Knative Eventing：CloudEvents → Trigger → KSvc）。
- 改造过程对用户**透明可观察**：通过 CR/Status/Event 告诉用户「我接管了你，副本数现在由 KPA 控制」。
- 提供**可逆**的开关：取消标签即可恢复成普通 Deployment。

### 1.2 非目标
- 不试图自动改造所有协议。Knative Serving 只支持 HTTP/1.1、HTTP/2（含 gRPC、WebSocket 受限），TCP/UDP/长连接服务不在本期范围内。
- 不在本期实现「跨集群迁移」「自定义自动伸缩算法替换 KPA」。
- 不在本期实现「无 Knative 也能用」的兜底（即假设集群已安装 Knative Serving，事件入口需要 Knative Eventing）。

---

## 2. 背景与可行性结论

### 2.1 Knative 关键组件回顾
- **Knative Serving**：核心对象 `Service (KSvc)` → 自动派生 `Configuration` → `Revision` → `Deployment` + `PodAutoscaler (KPA)`；前面挂 `Activator`（cold-start）+ `Queue-Proxy`（每 pod sidecar，统计并发）。
- **缩容到 0**：默认 `scale-to-zero-grace-period=30s`，无请求时 `Revision` 副本归零，下次请求由 Activator 接住并拉起。
- **Knative Eventing**：`Broker` + `Trigger` 把 CloudEvents 投递到 KSvc，天然适合「事件驱动唤醒」。

### 2.2 可行性结论
**可行，但本质是「替换」而不是「改造」原工作负载。** 因为：

| 项目 | 普通 Deployment | Knative Service |
|---|---|---|
| 副本控制 | HPA / 手动 `replicas` | KPA（必须） |
| 流量入口 | `Service` / `Ingress` | Knative 内置 ingress + Activator |
| 协议 | 任意 TCP | 仅 HTTP/HTTP2/gRPC |
| 镜像 spec | 完整 PodSpec | 受限 PodSpec（单容器 user-container 主导，部分字段被禁） |
| Owner | 用户 | KSvc 派生 |

所以 operator 不可能「就地把 Deployment 改成可缩容到 0」，必须创建一个**等价的 KSvc** 来承接流量，并停掉原 Deployment。

---

## 3. 触发匹配规则

### 3.1 匹配键（约定）
- Label：`ifaas.ifbiu.com/knative-autopilot=enabled`
- Annotation：
  - `ifaas.ifbiu.com/knative-mode=serving | eventing`（默认 `serving`；eventing 隐含 serving，不再单设 both）
  - `ifaas.ifbiu.com/knative-target-concurrency=10`（透传 KSvc `autoscaling.knative.dev/target`）
  - `ifaas.ifbiu.com/knative-min-scale=0`（**默认 0**，强制语义：业务必须提供 `/scaledownz` 才允许真正缩 0）
  - `ifaas.ifbiu.com/knative-max-scale=N`
  - `ifaas.ifbiu.com/knative-scaledown-probe-path=/scaledownz`（业务侧异步任务守卫接口路径，默认 `/scaledownz`）
  - `ifaas.ifbiu.com/knative-scaledown-probe-port=<port>`（默认沿用 user-port）
  - `ifaas.ifbiu.com/knative-scaledown-probe-interval=15s`（operator 轮询周期，默认 15s）
  - `ifaas.ifbiu.com/knative-eventing-broker=default`（mode=eventing 必填）
  - `ifaas.ifbiu.com/knative-eventing-filter-type=com.example.x`、`...-filter-source=...`

### 3.2 匹配对象
- 第一期只支持 `apps/v1 Deployment`。
- 后续可扩展到 `StatefulSet`（受限）、`Job/CronJob`（用 ksvc + KEDA event driven trigger 替代）。

### 3.3 接入方必须满足的契约
> 因为 `minScale=0` 是平台默认值，业务必须实现以下接口，否则会引起异步任务被中断。

- **路径**：`GET <scaledown-probe-path>`（默认 `/scaledownz`），监听 user-port 上。
- **返回语义**：
  - HTTP 200，body 必须能解析为布尔（约定 `{"allowScaleDown": true/false}` 或纯文本 `true|false`）。
  - `true`：当前实例没有未完成的异步任务，可被销毁。
  - `false`：仍有未完成任务，**禁止缩 0**。
- **超时与错误处理**：
  - operator 轮询超时（默认 2s）= 视为 `false`（保守），避免误杀。
  - 连续 N 次（默认 20 次≈5 分钟）`false` 会上报 Event，作为可观测信号。
- **示例（伪代码）**：

```text
GET /scaledownz
  if pendingTasks.count() == 0 -> 200 OK  {"allowScaleDown": true}
  else                          -> 200 OK  {"allowScaleDown": false}
```

> 这个接口在 readiness/liveness 之外单独定义，避免与 K8s 标准探针冲突。

---

## 4. 实现路径选型

三条路，必须选一条（或组合）：

### 4.1 路径 A：Mutating Admission Webhook 拦截创建
- 触发点：用户 `kubectl apply` Deployment 时 webhook 介入。
- 动作：把 Deployment 改写为 KSvc（或直接拒绝创建 Deployment + 转交给 operator 创建 KSvc）。
- 优点：用户视角最干净，不会产生「先有 Deployment、后被回收」的中间态。
- 缺点：
  - webhook 改写资源类型（Deployment → KSvc）超出 admission 的语义，业界一般做法是 **拒绝 + 由 controller 异步建 KSvc**，导致用户的 `kubectl get deploy` 找不到原对象，体验割裂。
  - 升级/回滚链路重；webhook 故障会阻塞所有 Deployment 创建。

### 4.2 路径 B：Controller Watch Deployment 异步接管（推荐）
- 触发点：controller 观察集群里所有 Deployment，过滤匹配 labels。
- 动作：
  1. 创建/更新一个同名 `KSvc`，PodSpec 翻译自原 Deployment；
  2. 将原 Deployment 的 `spec.replicas` 缩为 0（保留对象作为「源 manifest」），打 annotation `ifaas.ifbiu.com/managed-by=knative-autopilot`；
  3. 用一个 `KnativeAdoption` CR 作为「接管账本」，记录 owner、原始 hash、KSvc 名、状态条件；
  4. 用户取消 label → 删除 KSvc，把 Deployment `replicas` 还原。
- 优点：可观测性强、可逆、可灰度、admission 不掉链子。
- 缺点：存在「Deployment 启动 → 被缩到 0 → KSvc 拉起」的瞬时双跑窗口，需要明确不可避免。

### 4.3 路径 C：新增 CRD，用户直接声明
- 用户不再写 Deployment，而是直接写一个 `KnativeAdoption` CR。
- 优点：语义最干净。
- 缺点：违背用户「不改 YAML」的诉求，相当于另一种 KSvc 包装，价值有限。

**结论：路径 B 作为主线**，账本 CRD 命名为 `KnativeAdoption`（原 `IfResource` 弃用并删除）；CRD 不暴露给用户「写」，只承载 operator 内部状态与对外可观测的 status/condition。如果将来需要更强语义，再叠加路径 A 的 admission 阻断（仅校验，不改写）。

---

## 5. 体系结构与数据流

### 5.1 模块/对象关系（人话版）

```
用户                          ┌─────────────┐
  │ kubectl apply Deployment  │             │
  ▼ (带匹配 labels)            │   API       │
┌──────────────┐               │  Server     │
│ Deployment   │◄──────────────┤             │
│  raw-app     │               └─────┬───────┘
└──────────────┘                     │ watch
       ▲ 缩 0 / 还原                  ▼
       │                       ┌──────────────────────────┐
       │                       │ if-operator              │
       │                       │ ├─ DeploymentWatcher     │
       │                       │ ├─ Translator            │ Deployment → KSvc Spec
       │                       │ ├─ AdoptionReconciler    │ 维护 KnativeAdoption
       │                       │ ├─ ServiceSwapper        │ 接管原 Service ↔ KSvc 派生 Service
       │                       │ ├─ ScaleDownGuard        │ 走 pods/proxy 轮询 /scaledownz
       │                       │ └─ EventingReconciler    │ 仅 mode=eventing
       │                       └─────┬────────────────────┘
       │                             │ create/update
       │                             ▼
       │                       ┌────────────────┐
       └───── owner-ref ──────►│ KnativeAdoption│
                               │   CR (账本)     │
                               └─────┬──────────┘
                                     │ controls
                                     ▼
                               ┌──────────────┐    ┌──────────────┐
                               │ KSvc         │───►│ KPA + Pods   │
                               └─────┬────────┘    └──────┬───────┘
                                     │                    │ GET /scaledownz
                                     │                    ▼
                                     │              ScaleDownGuard
                          可选        ▼
                               ┌──────────────┐
                               │ Trigger      │◄── Broker ◄── CloudEvent
                               └──────────────┘
```

### 5.2 控制器状态机（KnativeAdoption 视角）

```
            ┌──────────┐  匹配到 Deployment
            │  Pending │──────────────────────┐
            └────┬─────┘                      ▼
                 │ KSvc 创建成功         ┌──────────────┐
                 ▼                       │  Translating │
            ┌──────────┐  Deployment   ◄─┴──────┬───────┘
            │ Adopting │  replicas=0           │
            └────┬─────┘  KSvc Ready            │
                 ▼
            ┌──────────┐  /scaledownz=false      ┌──────────────────┐
            │  Active  │ ───────────────────────►│ ScaleDownBlocked │
            │          │ ◄────── true ───────────│ (minScale=1)     │
            │          │                         └──────────────────┘
            └────┬─────┘  无请求 + true → KPA 缩 0
                 ▼
            ┌──────────────┐  新请求 / 事件
            │ ScaledToZero │───────────────► Activator 唤醒 → Active
            └────┬─────────┘
                 │ 用户去掉 label / 删除 Deployment
                 ▼
            ┌──────────┐
            │ Releasing│  删 KSvc，还原 replicas
            └────┬─────┘
                 ▼
              Deleted
```

Conditions（写回 `KnativeAdoption.status.conditions`，沿用 K8s 约定）：
- `Adopted`：KSvc 已创建并健康。
- `SourceQuiesced`：原 Deployment `replicas=0`。
- `ScaleDownAllowed`：最近一次 `/scaledownz` 轮询为 true（即允许缩 0）。
- `Ready`：`Adopted && SourceQuiesced` 都为 True。
- `Degraded`：翻译失败 / KSvc 不 Ready / Deployment 漂移 / `/scaledownz` 连续不可达。

---

## 6. PodSpec → KSvc 翻译规则

### 6.1 可直接搬运
- `containers[0].{image, command, args, env, envFrom, resources, ports[0]}`
- `imagePullSecrets`
- `serviceAccountName`
- `volumes` + `volumeMounts`（受 Knative 限制：仅 ConfigMap / Secret / EmptyDir / Projected，Knative 1.13+ 支持 PVC 但默认关闭）
- `nodeSelector` / `tolerations` / `affinity`（KSvc 通过 `feature-flags` 控制开放）

### 6.2 需要降级或拒绝
| 原字段 | 处理 |
|---|---|
| `replicas` | 丢弃；改写 `autoscaling.knative.dev/minScale`、`maxScale` |
| 多容器 | 第一期只支持 1 个主容器 + sidecar 通过 annotation 开放；多容器需校验 |
| `hostNetwork / hostPID / hostPort` | 拒绝接管（写 Degraded） |
| `livenessProbe / readinessProbe.tcpSocket(非 user-port)` | 翻译成 `userPort` probe |
| 非 HTTP 协议端口 | 拒绝接管，写 Condition `UnsupportedProtocol=True` |

### 6.3 流量入口
- **原 K8s `Service` 会被接管**（同名替换），客户端 DNS `<svc>.<ns>.svc.cluster.local` 行为保持不变，详见 §8.6。
- `KnativeAdoption.status.url` 同时暴露 KSvc 公网 URL，供 CI/CD 或外部网关使用。

---

## 7. Owner / 生命周期

- `KnativeAdoption` 用 `controller-runtime` 的 owner refs 拥有 `KSvc`（删除级联）。
- 反向：`KnativeAdoption` 通过 label selector / annotation 引用原 `Deployment`，但**不**作为 owner（避免删除账本时连带删除用户原始 Deployment）。
- Finalizer：`ifaas.ifbiu.com/restore-source`，在 `KnativeAdoption` 被删除时：
  1. 删 KSvc；
  2. 把原 Deployment `replicas` 还原为「接管前的快照值」（存放在 `KnativeAdoption.status.sourceSnapshot.replicas`）；
  3. 摘掉接管 annotation。

---

## 8. Knative Serving 与 Eventing 职责划分（核心）

> 一句话：**Serving 负责「让请求把 pod 拉起来并扩缩」；Eventing 负责「把外部事件翻译成对 Serving 的请求」。** 两者不是并列方案，而是「入口」+「执行体」的上下游关系。

### 8.1 一图概览

```
                ┌────────────────── 外部触发面 ──────────────────┐
                │                                                │
   外部用户/网关 ──HTTP──► (Serving 入口)                          │
   外部系统/MQ  ──事件──► Source ──► Broker ──► Trigger (Eventing)─┘
                                                  │
                                                  ▼ HTTP POST (CloudEvent)
                                          ┌────────────────────┐
                                          │ Knative Service    │  ←── Serving 域
                                          │  (Activator/Route) │
                                          │  └─ Revision N     │
                                          │     └─ KPA + Pods  │
                                          └────────────────────┘
                                                  ▲
                                          /scaledownz 反向影响 minScale
                                          (operator ScaleDownGuard)
```

### 8.2 职责切分表

| 关注点 | Knative Serving（必备） | Knative Eventing（mode=eventing 才需要） |
|---|---|---|
| 解决的问题 | 请求驱动的自动扩缩 + 缩到 0 + Activator 冷启动 | 把异步事件转成同步 HTTP 请求 |
| 主要 CRD | `Service`（→ Configuration → Revision → Deployment + KPA） | `Broker`、`Trigger`、`Source`（KafkaSource/PingSource/APIServerSource…） |
| 入口协议 | HTTP/1.1 / HTTP/2 / gRPC | CloudEvents（HTTP POST，`ce-*` header） |
| 唤醒来源 | 任何 HTTP 客户端 | Broker 内 dispatcher 投递 |
| 扩缩决策者 | KPA（基于并发 / RPS / cpu） | **仍然是 KPA**（事件落到 KSvc 后还是走 Serving） |
| 流量入口对象 | KSvc 自动派生的 `Service`（kn-route） | 不暴露给外部，仅由 Trigger 投递 |
| 持久化能力 | 无（请求路过即扩） | Broker 内部有 channel buffer（取决于实现，比如 MTChannel + Kafka） |
| 失败重试 | 客户端自己处理 | Trigger 内置重试 + DLS（Dead Letter Sink） |
| operator 创建什么 | `serving.knative.dev/v1 Service` | `eventing.knative.dev/v1 Trigger`（Broker 由平台提供，不创建） |
| 用户必须满足 | `/scaledownz` 接口 + HTTP 协议 + 单端口 | 上述 + 能正确处理 CloudEvent header / body |
| RBAC 需求 | `services.serving.knative.dev`（含 status） | + `triggers.eventing.knative.dev`、`brokers.eventing.knative.dev` (get/list) |
| 失败回退 | 没装 Serving → 拒接管（前置校验） | 没装 Eventing / Broker 不存在 → 退化为 serving-only + Degraded |

### 8.3 在本 operator 内部的子模块切分

按职责把 controller 内部拆为 4 个子 reconciler，**每个只读写自己关心的对象**：

| 子模块 | 关心的对象 | 工作 |
|---|---|---|
| `DeploymentWatcher` | Deployment（带匹配 label） | 发现/解除匹配 → 创建/删除 `KnativeAdoption` |
| `AdoptionReconciler` (Serving 半边) | KnativeAdoption, KSvc, 原 Deployment | 翻译 PodSpec → KSvc；缩 0 原 Deployment；写 Conditions |
| `ServiceSwapper` | 原 Service, KSvc 派生 Service | 接管前快照原 Service、删除占名、还原；维护 `ServiceAdopted` condition |
| `EventingReconciler` (Eventing 半边) | KnativeAdoption, Trigger | 仅当 `mode=eventing`：根据 annotation 创建/更新/删除 `Trigger`，subscriber 指向同名 KSvc |
| `ScaleDownGuard` | KSvc + KnativeAdoption | 通过 apiserver `pods/proxy` 子资源 GET `/scaledownz`，根据返回值在 KSvc 上 patch `autoscaling.knative.dev/min-scale=0|1` |

> `EventingReconciler` 与 `AdoptionReconciler` **解耦**：它假设 KSvc 已经存在；不存在就什么都不做（写 `EventingPending`）。这保证 mode=serving 用户完全不会引入 Eventing 依赖。

### 8.4 二者交互边界（关键约定）

1. **Eventing 不直接管 pod**。Trigger 的 subscriber 永远是 KSvc 的 URL，事件最终通过 Activator 拉起 pod。所以「缩到 0 → 事件来 → 拉起」也是走 Serving 的链路。
2. **不允许 Eventing 模式禁用 Serving**。`mode=eventing` 实际等价于「Serving + Trigger」，operator 内部按这个语义生成对象。
3. **Trigger 的 reply 策略**：第一期一律不配置 reply（事件单向）。如果业务需要 reply chain，留 P2。
4. **Broker 不在 operator 管辖范围**。Broker 由平台预先建好，operator 只引用 `eventing-broker` annotation 给的名字；找不到就降级 + Event 提示。
5. **失败语义**：Trigger 投递失败的重试由 Eventing 自己保证；KSvc 自身扩容失败由 Serving 报告 Revision 状态。operator 只把两侧状态汇总到 `KnativeAdoption.status.conditions`，不重复实现重试。

### 8.5 `/scaledownz` 守卫机制详解

`/scaledownz` 是接入方契约，**KPA 本身不感知它**，所以由 operator 的 `ScaleDownGuard` 子模块作为「策略层」夹在中间。**探测一律走 apiserver 的 `pods/proxy` 子资源**，不直连 pod IP。原因：
- 不依赖 operator 与 pod 之间的网络可达性（跨 NetworkPolicy / 不同 CNI / 跨节点都 OK）；
- 鉴权直接走 K8s RBAC，免单独维护证书或 token；
- 多租户友好：可以按 namespace 收敛权限。
- 代价：多一跳 apiserver，会消耗 apiserver QPS；默认 15s 间隔 + 每对象 1~2 个 pod 的探测体量足够低。

```
ScaleDownGuard 周期循环（默认每 15s）：
  for each KnativeAdoption in Active:
      pods = list pods of KSvc latest Revision (label selector: serving.knative.dev/revision=<rev>)
      if len(pods) == 0:                # 已经缩到 0
          set Condition ScaleDownAllowed=Unknown
          continue
      results = parallel call:
          GET /api/v1/namespaces/<ns>/pods/<pod>:<port>/proxy/<probePath>   (timeout 2s)
      allAllow = all(parseBool(results) == true)
      if allAllow:
          patch KSvc annotation autoscaling.knative.dev/min-scale = max(userMin, "0")
          set Condition ScaleDownAllowed=True
      else:
          patch KSvc annotation autoscaling.knative.dev/min-scale = max(userMin, "1")
          set Condition ScaleDownAllowed=False, reason=PendingTasks
```

要点：
- **默认假设保守**：探测失败（超时 / 404 / 5xx / RBAC 拒绝）一律按 `false` 处理，宁可不缩也不杀任务。
- **min-scale 的写入只覆盖动态值**，用户在 annotation 里显式指定的 `knative-min-scale=2` 会作为下界（`max(用户值, 守卫值)`）。
- **probe port 选择**：默认沿用 KSvc user-port；可以通过 annotation `ifaas.ifbiu.com/knative-scaledown-probe-port` 指定一个**独立侧通道端口**，避免 user-port 在高并发时影响业务。容器要同时监听该端口才行。
- **不能用 readiness 代替**：readiness 失败会让 Activator 不路由，反而让 KPA 立刻视为「无 active pod」并加快缩零。`/scaledownz` 必须是独立路径。
- **PreStop 钩子是兜底**：translator 给 KSvc 注入一个 `preStop` httpGet `/scaledownz`；如果业务任务还在跑，preStop 阻塞直到完成或 `terminationGracePeriodSeconds` 超时。
- **指标**：`autopilot_scaledown_blocked_total{namespace,adoption}`、`autopilot_scaledownz_probe_errors_total{reason}`、`autopilot_scaledownz_probe_latency_seconds{quantile}`。

### 8.6 原 Service 同名冲突的接管策略（无感知方案）

#### 8.6.1 冲突来源
Knative `Service`（KSvc）会自动派生**两个**同 namespace 下的 K8s `Service`：

| Knative 派生对象 | 名字 | 类型 | 作用 |
|---|---|---|---|
| 公共入口 Service | `<ksvc-name>` | ExternalName / ClusterIP | 客户端用这个名字访问，CNAME 到 ingress |
| 私有内部 Service | `<ksvc-name>-private` | ClusterIP | 直连 Revision pods（绕过 Activator） |

如果用户原 `Deployment` 同 namespace 已经存在名为 `<ksvc-name>` 的 `Service`，Knative 控制器会创建失败，KSvc 永远 NotReady。

#### 8.6.2 目标：客户端 `<svc>.<ns>.svc.cluster.local` 解析行为不变

候选方案三选一：

**方案 A：删原 Service，让 KSvc 直接占名（推荐，M1 默认）**

数据流变化：

```
接管前:                                接管后:
client -> Service(<svc>) -> Pods   client -> Service(<svc>) (KSvc 派生 ExternalName)
                                          -> Knative ingress
                                          -> Activator/Route -> Revision Pods
```

operator 流程：
```
1. 在 KnativeAdoption.status.sourceSnapshot.service 里完整快照原 Service spec
   (含 selector / ports / sessionAffinity / annotations / labels)
2. 给原 Service 打 finalizer: ifaas.ifbiu.com/restore-source-service
3. delete Service <svc>            # 进入约 1~3s 服务中断窗口
4. create KSvc <svc>               # Knative 控制器创建同名派生 Service
5. 等 KnativeAdoption.status.URL ready
6. 撤销时反过来:
   delete KSvc -> 等派生 Service 消失 -> 用 sourceSnapshot 重建原 Service -> 摘 finalizer
```

优点：
- 客户端 DNS 名字不变，**真正无感知**；端口、协议、cluster.local 域行为都一致。
- 实现简单，不需要维护 Endpoints。
- 不破坏 KSvc 自身的派生关系（KSvc 完全按 Knative 默认方式工作）。

缺点 / 风险：
- 删除 + 重建期间存在**短暂中断窗口**（典型 < 3s）。
- 与 GitOps（ArgoCD/Flux）冲突：用户 Application 里写过原 Service，会被反复 reconcile 拉回，导致和 KSvc 派生 Service 互相覆盖。
- 用户原 Service 字段（如 `loadBalancerIP`、`sessionAffinity`）丢失，KSvc 派生 Service 不支持这些字段。

GitOps 协同（必须项）：
- 接管时在 `KnativeAdoption.status` 写出**给 GitOps 用的 ignore 片段**，文档化让用户拷到 Application：
  ```yaml
  ignoreDifferences:
  - group: ""
    kind: Service
    name: <svc>
    jsonPointers: ["/spec", "/metadata/annotations", "/metadata/labels"]
  ```
- operator 同时给原 Service 资源（在删除前的那一刻）和 KSvc 都打上 annotation `argocd.argoproj.io/sync-options=Prune=false,Replace=true`，作为兜底。

**方案 B：保留原 Service，改成 selector-less + 维护 EndpointSlice（无中断，但复杂）**

数据流变化：

```
client -> Service(<svc>, selector-less)
          ↑ EndpointSlice 由 operator 维护
          └── address: KSvc <svc>-knative-private 的 ClusterIP
```

operator 流程：
```
1. KSvc 改名命名为 <svc>-knative (避免占名)
2. 把原 Service 改写为:
     spec.selector = nil
     spec.ports 对齐 KSvc user-port
   (Service.spec.type 不能改，必须维持原 ClusterIP)
3. 维护一个 EndpointSlice <svc>-adopt:
     addresses = [KSvc <svc>-knative-private 的 ClusterIP]
     ports = [user-port]
4. watch KSvc -> private Service 的 ClusterIP 变化 -> 重写 EndpointSlice
5. 撤销时把 selector 还原 + 删 EndpointSlice + 删 KSvc
```

优点：
- **零中断窗口**：原 Service 自始至终都在。
- 用户原 Service 上挂的 NetworkPolicy / monitor / annotation 全部保留。
- GitOps 看到的 Service 还是「那一个」，diff 面积更小（仅 selector 字段差异，可 ignore）。

缺点：
- operator 必须**自己实现 4 层负载到 KSvc 的转发**：本质是「指向 private Service ClusterIP」，但 private Service 的 ClusterIP 一般稳定（除非删除重建），需 watch。
- 绕过了 Activator，**失去 cold-start 唤醒能力**——这条是死路。因为 `<ksvc>-private` 只在 pod 存在时有 endpoint，缩到 0 后访问会 503。
- 因此方案 B **只在 minScale ≥ 1 时可用**，与本设计「缩到 0」的目标冲突，列为备选不采纳。

**方案 C：保留原 Service 改成 ExternalName，指向 KSvc 派生 Service（轻量但有协议限制）**

数据流变化：

```
client -> Service(<svc>, type=ExternalName) --CNAME--> <svc>-knative.<ns>.svc.cluster.local
                                                            -> Knative ingress -> Activator
```

operator 流程：
```
1. KSvc 命名为 <svc>-knative
2. 备份原 Service spec
3. 重建原 Service: type=ExternalName, externalName=<svc>-knative.<ns>.svc.cluster.local
   (Service.spec.type 不可变 -> 实际仍需 delete + recreate, 中断窗口同方案 A)
4. 撤销时还原
```

优点：
- 客户端 DNS 名字保留；调用语义看起来「最像 ExternalName 别名」。

缺点：
- ExternalName 实际是 CNAME，客户端解析后 Host header 会变成目标名字。HTTP/1.x 调用通常 OK，但 **gRPC、TLS SNI、需要 Host: 原名 的服务会异常**。
- type 变更同样要 delete + recreate，没解决方案 A 的中断窗口问题。
- 维护两份 KSvc 命名规则，账本和调用链变复杂。

#### 8.6.3 推荐落地

| 期次 | 默认策略 | 备注 |
|---|---|---|
| M1 | 方案 A | 拒接管「namespace 内已有同名 Service 且不带 `ifaas.ifbiu.com/managed-by=knative-autopilot` annotation」之外的高风险场景：原 Service 是 `LoadBalancer`/`NodePort`、或被 K8s 系统 owner 持有时直接拒绝接管 |
| M2 | 方案 A + 接管预热 | 接管前临时 `kubectl scale --replicas=同副本数` 拉起 KSvc Revision，再做 swap，把中断窗口控制在亚秒级 |
| M3 | 方案 A 兜底 + 提供 opt-in 方案 C | 给少数严格要求「零删除」的场景一个 ExternalName 路径 |

#### 8.6.4 配套契约
- KSvc 与原 Service 必须**同 namespace 同名**，否则放弃接管同名 Service（KSvc 仍可创建带后缀的名字，但客户端要改地址，丧失"无感知"）。
- 原 Service 接管也要写 finalizer，独立于 Deployment 还原：`ifaas.ifbiu.com/restore-source-service`。
- Condition 新增：`ServiceAdopted`（True=原 Service 已被替换；False=未接管 / 失败；Unknown=接管中）。

### 8.7 ScaleDownGuard 的合并去抖（namespace-level batching）

**问题**：守卫对每个 `KnativeAdoption` 独立轮询，namespace 内有 N 个对象就有 N 路并行 patch；当一波批量发布或事件风暴让多个对象同时翻转 `allowScaleDown` 时，apiserver 会被打成 PATCH 风暴。

**设计**：守卫不再让单对象直接 patch KSvc，而是把「期望的 minScale」写到一个 namespace 级别的 workqueue，由一个 namespace 专属 worker 合并 flush。

```
┌────────────────┐  decision      ┌─────────────────────┐
│ Per-Adoption   │ ──────────────►│ Namespace Flusher   │
│ Probe Loop     │  enqueue       │  (debounce + batch) │
└────────────────┘                └──────────┬──────────┘
                                             │ flush every X ms
                                             ▼
                                   parallel patch KSvc(s)
                                   (capped concurrency)
```

约束：
- **去抖窗口**：每 namespace 默认 `flushInterval=2s`；窗口内对同一 KSvc 的多次决策只保留最后一个（`minScale=1` 优先级高于 `0`，避免在抖动中错杀任务）。
- **并发上限**：每 namespace 同时 in-flight 的 KSvc patch 不超过 `maxInFlight=5`，超出排队。
- **patch idempotent**：只在「当前 KSvc annotation 与目标值不一致」时才发 PATCH，避免无效写。
- **失败重试**：失败按指数退避入队；连续失败 5 次升级为 Event + Degraded。
- **优先级**：`minScale=1`（保护任务）走快速通道，独立于 `minScale=0` 的批次。
- **可观测性**：
  - `autopilot_guard_flush_total{namespace,result}`
  - `autopilot_guard_queue_depth{namespace}`
  - `autopilot_guard_patch_inflight{namespace}`

> 实现层：用 `client-go workqueue.RateLimiting` + 每 namespace 一个 goroutine。Reconciler 不再持有这个 worker，归 `ScaleDownGuard` 子模块管理。

### 8.8 KSvc 与 Trigger 的订阅关系（多对一影响分析）

**问题陈述**：一个 KSvc 是否允许被同 namespace 内多个 `Trigger` 订阅？

#### 8.8.1 允许多对一（默认放开）

行为：

```
Broker --(filter A)--> Trigger A ──┐
Broker --(filter B)--> Trigger B ──┼──► KSvc <svc>
Broker --(filter C)--> Trigger C ──┘
```

影响：
- ✅ **业务表达力强**：同一个服务可以处理多种不同事件类型（订单、退款、退货走同一个 KSvc 不同 handler）。
- ✅ **资源利用率高**：复用同一组 pods + 缩到 0 行为统一；活跃判断由 Activator 看所有 Trigger 投递的并发数综合决定。
- ⚠️ **故障域耦合**：任何一个 Trigger 投递异常引起的 backpressure 都会把 KSvc 推到忙状态；DLS（Dead Letter Sink）必须分 Trigger 配置否则会窜。
- ⚠️ **operator 账本变复杂**：一个 `KnativeAdoption` 可能不再是 Trigger 的唯一 owner；GC 时要确认所有「我创建的 Trigger」都被清掉，不能误删用户手建的 Trigger。
- ⚠️ **CloudEvent 调试更难**：业务收到一条 event 时无法 100% 判定经过哪个 Trigger 链路（除非业务自己看 `ce-knativetrigger` header）。

#### 8.8.2 禁止多对一（强制 1:1）

行为：

```
KnativeAdoption ─── 1:1 ──► Trigger (operator owned) ──► KSvc
```

影响：
- ✅ **账本极简**：一个 `KnativeAdoption` 拥有唯一 Trigger，所有权清晰、删除安全。
- ✅ **错误定位容易**：CloudEvent 链路只有一条。
- ❌ **业务侧被迫多写几个 KSvc**：原本一个服务多种 event 类型的场景，需要拆成多个 KSvc 或者业务自己再多路复用，违背"接管原 Deployment"的目标。
- ❌ **用户已手建的 Trigger 会与 operator 冲突**：operator 创建同名 Trigger 时会失败，需要额外的「合并接管」逻辑。
- ❌ **与 Knative Eventing 自身能力背离**：Trigger 本来就是为"多过滤器、多订阅者"设计的。

#### 8.8.3 决策

**允许多对一，但有约束**：
1. operator 创建的 Trigger 一律命名 `<adoption>-<filterHash>`，带 owner label `ifaas.ifbiu.com/owner=<adoption>`；删除时只 GC 带这个 label 的 Trigger。
2. **不接管**用户手建的、指向同名 KSvc 的 Trigger（没有 owner label 的视为外部资产，operator 只读不写）。
3. `KnativeAdoption.spec.eventing` 支持声明多组 filter：

   ```yaml
   eventing:
     broker: default
     filters:
     - type: com.example.order.created
     - type: com.example.refund.created
       source: /payments
   ```

   每组 filter 对应一个 operator 管理的 Trigger。
4. DLS 留 P2：M1 不配置，靠 Knative 默认重试。
5. Conditions：`EventingReady = all owner-labeled Triggers Ready`。

---

## 9. 需要考虑的问题清单

1. **协议兼容**：必须在接管前校验端口/协议，不兼容直接拒接管 + Event 告诉用户。
2. **冷启动延迟**：缩到 0 后第一次请求会经过 Activator，p99 通常 1~3 秒，需文档化。
3. **`/scaledownz` 缺失/异常**：默认 `minScale=0` 的前提是业务实现该接口。operator 校验在接管前发起一次预检（HEAD 或 GET），失败则拒接管并提示。
4. **PreStop 阻塞过长**：`terminationGracePeriodSeconds` 必须给足（默认建议 ≥ 业务最长异步任务时长 + 5s），否则 K8s 仍会强杀。可在 annotation 里覆盖。
5. **原 Service 接管的中断窗口**：方案 A 删除 + KSvc 派生重建之间存在 1~3s 中断；详见 §8.6，M2 通过「先拉起 Revision 再 swap」收敛到亚秒级。
6. **配置漂移**：用户改了原 Deployment.spec，要不要同步到 KSvc？方案：watch Deployment 的 spec hash，发生变化 → 重新翻译 → 滚 KSvc 新 Revision。
7. **HPA / KEDA 冲突**：如果原 Deployment 挂了 HPA，要在接管时拒绝或先解绑。
8. **GitOps 冲突**：ArgoCD/Flux 会反复把 Deployment `replicas` 与原 Service 拉回。需要：
   - 在 KnativeAdoption.status 输出建议的 `ignoreDifferences` 片段；
   - 接管时给被托管对象打 `argocd.argoproj.io/sync-options=Prune=false,Replace=true`；
   - 文档化要求用户在 Application 加 `ignoreDifferences`。
9. **RBAC 与多租户**：operator 需要 cluster-wide 的 Deployment R/W、KSvc R/W、Trigger R/W（仅 mode=eventing 命名空间生效）；建议加 namespace 白名单。
10. **可观测性**：metrics 至少导出
   - `autopilot_adopted_total{namespace}`
   - `autopilot_translation_errors_total{reason}`
   - `autopilot_revision_age_seconds`
   - `autopilot_scaledown_blocked_total{namespace,adoption}`
   - `autopilot_scaledownz_probe_errors_total{reason}`
11. **回滚路径**：用户摘掉 label 后必须能恢复（见 finalizer），并提供 `kubectl annotate ... autopilot.pause=true` 的暂停开关。
12. **Webhook 校验**（P2）：补一个 ValidatingWebhook，禁止用户手动写 `ifaas.ifbiu.com/managed-by` annotation，避免账本被伪造。
13. **Knative 未安装**：operator 启动时探测 `serving.knative.dev/v1` CRD 是否存在，缺失则只发 Event、不 reconcile。
14. **事件驱动唤醒**：若 mode=eventing，需要在同 namespace 自动创建 `Trigger`，过滤条件来自 annotation；Broker 必须事先存在。
15. **资源配额**：KSvc 会临时拉起 Activator 代理流量，注意 namespace ResourceQuota。
16. **安全上下文**：KSvc 强制 `runAsNonRoot`、禁止 `privileged`，要预先校验。
17. **Sidecar（istio / linkerd）**：注入策略要在 KSvc spec template 里同步保留 annotation。
18. **多 pod 时 `/scaledownz` 投票策略**：守卫采用「全部 true 才允许缩」，部分 false 时整体保守；需写进文档让接入方理解。

---

## 10. 改造清单（落到本仓库的工作项）

### 10.1 API 层（CRD 重命名 IfResource → KnativeAdoption）
- [ ] `kubebuilder create api --group cache.ifbiu.com --version v1alpha1 --kind KnativeAdoption`
- [ ] 删除旧 `IfResource` 相关：`api/v1alpha1/ifresource_types.go`、`internal/controller/ifresource_controller*.go`、`config/crd/bases/cache.ifbiu.com_ifresources.yaml`、`config/samples/cache_v1alpha1_ifresource.yaml`，并在 `PROJECT` 里删除该 resource 条目（用 `kubebuilder edit` 流程，不手编 PROJECT）。
- [ ] 新 `KnativeAdoption.Spec`：
  - `Source`：`{kind: Deployment, name, namespace}`
  - `Mode`：`serving | eventing`
  - `Autoscaling`：`{minScale (default 0), maxScale, targetConcurrency}`
  - `ScaleDownProbe`：`{path (default /scaledownz), port, intervalSeconds, timeoutSeconds, consecutiveFailureThreshold}`
  - `Eventing`：`{broker, filterType, filterSource}` （mode=eventing 才校验）
- [ ] `KnativeAdoption.Status`：
  - `Conditions`、`URL`、`SourceSnapshot.Replicas`、`LastScaleDownProbe`（time + result + message）、`ObservedSourceHash`
- [ ] `make manifests && make generate`

### 10.2 Controller 层
- [ ] 拆 5 个 reconciler，每个一个文件（同 `internal/controller/` 目录）：
  - `deployment_watcher.go`：watch `apps/v1 Deployment`，匹配 label → 创建/更新对应 `KnativeAdoption`。
  - `adoption_reconciler.go`：以 `KnativeAdoption` 为中心，翻译 PodSpec → KSvc；缩 0 / 还原原 Deployment；写 Conditions。
  - `service_swapper.go`：原 Service 快照 / 删除 / KSvc 派生 Service 占名 / 撤销时还原（详见 §8.6）。
  - `eventing_reconciler.go`：mode=eventing 时维护 `Trigger`。
  - `scaledown_guard.go`：通过 `pods/proxy` 轮询 `/scaledownz`，patch KSvc minScale。
- [ ] 纯函数包 `internal/translator`：`Translate(dep *appsv1.Deployment, adoption *cachev1alpha1.KnativeAdoption) (*kservingv1.Service, error)`，方便单测。
- [ ] 在 translator 自动注入 `lifecycle.preStop` httpGet `/scaledownz`，以及合理的 `terminationGracePeriodSeconds`。
- [ ] Finalizers：
  - `ifaas.ifbiu.com/restore-source`：还原 Deployment replicas。
  - `ifaas.ifbiu.com/restore-source-service`：还原原 Service spec。

### 10.3 RBAC
- [ ] markers：
  - `apps/deployments`（含 status）get;list;watch;update;patch
  - `serving.knative.dev/services`（含 status）全权限
  - `eventing.knative.dev/triggers` 全权限
  - `eventing.knative.dev/brokers` get;list（仅校验存在）
  - `core/services` get;list;watch;create;update;patch;delete（接管原 Service）
  - `core/pods` get;list（守卫探测列出目标 pod）
  - `core/pods/proxy` get（**`/scaledownz` 探测路径**，通过 apiserver 代理）
  - `core/events` create;patch
- [ ] `make manifests` 重新生成 role.yaml。

### 10.4 Webhook（**M1 必备**）
- [ ] `kubebuilder create webhook --group cache.ifbiu.com --version v1alpha1 --kind KnativeAdoption --programmatic-validation`
- [ ] ValidatingWebhook 校验项：
  - 禁止用户手动写带 `ifaas.ifbiu.com/managed-by`、`ifaas.ifbiu.com/owner` 的 annotation/label（防伪账本）。
  - `KnativeAdoption.spec.source` 引用的 Deployment 必须存在且未挂 HPA。
  - 接管前对原 Deployment 第一容器的端口/协议预检（拒绝 hostNetwork / hostPort / 非 HTTP）。
  - 接管前对样本 pod（如果有）做一次 `/scaledownz` 预检（HEAD 或 GET，超时 2s），失败拒绝接管。
  - `mode=eventing` 时 broker annotation 不为空。
- [ ] MutatingWebhook（可选 / P2）：自动注入推荐 annotation 默认值（`min-scale=0`、`target=10`、`scaledown-probe-path=/scaledownz`）。
- [ ] 同时在 Deployment 上挂一个**轻量 Validating Webhook**：拦截「带 autopilot label 的 Deployment 被用户手动改 `spec.replicas`」，避免与 ScaleDownGuard 互斗。

### 10.5 测试
- [ ] 单元测试覆盖 `translator` 各分支（含 preStop 注入、port 选择）。
- [ ] 单元测试覆盖 `scaledown_guard` 投票逻辑（all true / 任一 false / 探测失败 / pods/proxy 403）。
- [ ] 单元测试覆盖 `service_swapper`：快照、删除、占名、还原、finalizer 清理。
- [ ] envtest：模拟 Deployment + Service apply → KnativeAdoption 创建 → 原 Service 被删 → KSvc 派生 Service 占名 → Deployment replicas=0。
- [ ] e2e（kind + knative serving + 一个返回可控的 `/scaledownz` 测试镜像）：完整跑一遍冷启动 + 守卫阻止缩 0 + 流量从原 ClusterIP 切到 KSvc 路径。

### 10.6 文档与样例
- [ ] `config/samples/cache_v1alpha1_knativeadoption.yaml`：示意一个被接管的 Deployment（带 label）+ 对应自动生成的 KnativeAdoption。
- [ ] README 增加「Knative Autopilot」章节，链接到本文档。
- [ ] 单独一篇接入指南：「如何实现 `/scaledownz`」。

---

## 11. 分期路线

- **M1（最小可用）**：路径 B + Serving-only + Deployment 单容器 + `/scaledownz` 守卫（pods/proxy + namespace 去抖）+ ServiceSwapper（方案 A）+ ValidatingWebhook（KnativeAdoption + Deployment 双对象）+ finalizer 还原。
- **M2**：Eventing 模式（多 Trigger，1:N filter）+ 镜像预拉（warm-up KSvc）+ 多容器/PVC + GitOps 适配模板生成。
- **M3**：opt-in 方案 C（ExternalName 软接管）+ MutatingWebhook 默认值注入 + 多租户白名单 + metrics 完整化。
- **M4**：跨工作负载类型（StatefulSet / Job）；Job 通过 KEDA `ScaledJob` 二选一。

---

## 12. 已验证 / 未决议题

### 12.1 已验证：M2「预热」的可达上限
> 调研结论：**KSvc 派生的同名 K8s Service 是死结，无法做到零中断切换**。`networking.knative.dev/visibility=cluster-local` 只能控制是否发布到外部 gateway，**同名 K8s Service 一定会被 Knative 控制器创建**，因此原 Service 与 KSvc 派生 Service 的同名冲突无法通过「预先创建 cluster-local KSvc」绕开。
>
> M2 预热的实际目标因此修正为「**镜像/依赖预热**」，而非「零中断」：
> - 在接管前用一个临时辅助资源（如带后缀名字的 Deployment，或直接 `imagePullPolicy` 触发 prePull DaemonSet）把镜像拉到目标节点；
> - 接管时 KSvc 第一个 Revision 的 pod 启动跳过镜像拉取耗时；
> - 中断窗口仍存在（Service 删除 → KSvc 派生 Service 创建），实测可压到 ~1s 内。
>
> 参考资料：[Knative private services 文档](https://knative.dev/docs/serving/services/private-services/)、社区共识「Knative 无原生 pre-started pod / warm pool」。

### 12.2 剩余未决议题

1. **多容器场景**（M2）：Knative 1.x 支持 sidecar 但有受限端口/probe 规则，需要 translator 决定「拒接管 vs 自动选主容器」。倾向：>1 容器且未通过 annotation 指定主容器 → 拒接管。
2. **跨 namespace Broker 引用**：是否允许 `mode=eventing` 引用别的 namespace 的 Broker？Knative 默认不支持，但有人用 BrokerRef + 自定义 ingress 绕过。M1 不支持，未来按需。
3. **PVC 接管**：Knative 1.13+ 可开 PVC feature flag，但默认禁用。Operator 是否在检测到原 Deployment 有 PVC 时自动开 feature flag？倾向 不自动开，拒接管 + Event 提示。

> 已决议项（不再列入）：
> - CRD 名定为 `KnativeAdoption`。
> - 默认 `minScale=0`，业务必须实现 `/scaledownz`（GET，返回布尔），由 operator 守卫动态调整 minScale。
> - mode 枚举确定为 `serving | eventing` 两值。
> - `/scaledownz` 探测**走 `pods/proxy` 子资源**，不直连 pod IP。
> - 原 Service 同名冲突采用 **方案 A：删除原 Service，让 KSvc 派生 Service 占名**，搭配 ServiceSwapper + finalizer 还原；中断窗口 ~1-3s 不可避免，M2 通过镜像预拉压到 ~1s 内。
> - 守卫 patch min-scale 采用 **namespace 级别合并去抖**（每 2s flush，`minScale=1` 优先，maxInFlight=5）。
> - **允许一个 KSvc 被多个 operator-owned Trigger 订阅**（命名带 filter hash + owner label）；不接管用户手建 Trigger。
> - **ValidatingWebhook 列入 M1 必备**，覆盖 KnativeAdoption 与 Deployment 两个对象。