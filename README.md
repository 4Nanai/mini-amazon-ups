# Amazon 与 UPS 通信流程

本文档基于系统定义的 gRPC 协议与 World 模拟器交互协议，分别从 Amazon 和 UPS 的视角梳理了订单生命周期中的关键消息传递流程。

## 📦 Amazon 视角

Amazon 在系统中扮演 `UpsService` 的客户端以及 `AmazonService` 的服务端。同时，它通过 TCP 连接发送 `ACommands` 和接收 `AResponses` 与 World 模拟器进行交互。

### 1. 接收订单与准备打包
* **通知 UPS：** 客户购买商品 (Buy) 后，Amazon 调用 UPS 的 `RequestPickup` (gRPC) 方法，请求取件。
* **接收 UPS 响应：** Amazon 收到 `PickupResp`，获取分配给该订单的卡车信息。
* **通知 World 打包：** Amazon 向 World 发送包含 `APack` 指令的命令（对应流程图中的 `AToPac`），指示仓库开始打包商品。

### 2. 卡车抵达与货物装车
* **接收卡车抵达通知：** UPS 的卡车到达仓库后，Amazon 会收到来自 UPS 的 `NotifyTruckArrived` (gRPC) 调用 。
* **通知 World 装车：** 确认卡车抵达后，Amazon 向 World 发送 `APutOnTruck` 指令，将包裹装载到指定的卡车上。

### 3. 通知 UPS 发车
* **通知 UPS 可以出发：** 收到打包并装车完成的确认后，Amazon 调用 UPS 的 `NotifyLoadReady` (gRPC) 方法，告知 UPS 该包裹已准备就绪。

### 4. 追踪配送状态
* **开始配送：** UPS 发车后，Amazon 会收到 UPS 发来的 `NotifyDeliveryStarted` (gRPC) 调用。
* **配送完成：** 货物送达后，Amazon 收到 UPS 发来的 `NotifyDeliveryComplete` (gRPC) 调用，完成整个订单流程。

### 5. 异常处理
* **取消订单或修改地址：** 发生外部 Cancel 或 Redirect 事件时，Amazon 可通过 gRPC 调用 UPS 的 `RequestCancel` 或 `RequestRedirect` 传递修改请求。

---

## 🚚 UPS 视角

UPS 在系统中扮演 `UpsService` 的服务端以及 `AmazonService` 的客户端。同时，它通过 TCP 连接发送 `UCommands` 和接收 `UResponses` 与 World 模拟器进行交互。

### 1. 响应取件请求
* **接收取件请求：** UPS 收到来自 Amazon 的 `RequestPickup` (gRPC) 调用。
* **调度卡车：** UPS 向 World 发送 `UGoPickup` 指令，派遣卡车前往指定仓库。
* **回复 Amazon：** UPS 向 Amazon 返回 `PickupResp` 响应，确认取件任务。

### 2. 抵达仓库与等待装车
* **通知 Amazon 卡车抵达：** 卡车抵达仓库后，UPS 调用 Amazon 的 `NotifyTruckArrived` (gRPC) 方法，告知对方卡车已就位，可以装车。

### 3. 开始配送
* **接收发车许可：** UPS 收到来自 Amazon 的 `NotifyLoadReady` (gRPC) 调用，得知货物已装载完毕。
* **指示卡车配送：** UPS 向 World 发送 `UGoDeliver` 指令，指示卡车前往客户目的地。
* **更新配送状态：** 同时，UPS 调用 Amazon 的 `NotifyDeliveryStarted` (gRPC) 方法，通知 Amazon 配送已开始。

### 4. 完成配送
* **通知 Amazon 订单完成：** 配送完成后，UPS 调用 Amazon 的 `NotifyDeliveryComplete` (gRPC) 方法，闭环订单。

### 5. 异常处理
* **处理请求：** UPS 处理来自 Amazon 的取消或重定向请求，并根据当前状态返回 `CancelResp` 或 `RedirectResp` 结果。