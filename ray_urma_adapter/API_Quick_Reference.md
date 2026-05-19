# RAY RDT 英伟达API vs UMDK API 快速对照表

## 一、内存注册API对照

| RAY/NIXL API | 英伟达API | UMDK URMA API | 差异 | 适配方案 |
|--------------|----------|--------------|------|---------|
| `nixl_agent.register_memory([addr, size, gpu_id])` | GDRCopy/cuMemRegister | `urma_register_seg(ctx, seg_cfg)` | NIXL简化参数，URMA需seg_cfg结构体 | 封装UrmaAgent.register_memory()自动构建seg_cfg |
| `nixl_agent.deregister_memory(handle)` | cuMemUnregister | `urma_unregister_seg(ctx, seg)` | 无差异 | 直接映射 |
| `nixl_agent.get_xfer_descs()` | NIXL内部序列化 | `urma_serialize_seg(seg)` | 格式不同 | 实现get_xfer_descs()调用serialize |

## 二、传输操作API对照

| RAY/NIXL API | 英伟达API | UMDK URMA API | 差异 | 适配方案 |
|--------------|----------|--------------|------|---------|
| `nixl_agent.transfer(READ)` | RDMA READ via UCX | `urma_read(jfs, remote_jetty, local_seg, remote_seg, ...)` | NIXL隐藏jfs/jetty细节 | UrmaAgent.read()封装 |
| `nixl_agent.transfer(WRITE)` | RDMA WRITE via UCX | `urma_write(jfs, remote_jetty, ...)` | 同上 | UrmaAgent.write()封装 |
| `nixl_agent.check_xfer_state()` | UCX completion check | `urma_poll_jfc(jfc)` | NIXL返回状态字符串，URMA返回CR列表 | check_xfer_state()转换状态 |

## 三、端点管理API对照

| RAY/NIXL API | 英伟达API | UMDK URMA API | 差异 | 适配方案 |
|--------------|----------|--------------|------|---------|
| `nixl_agent()` | UCX context init | `urma_init()` + `urma_create_context()` | NIXL单一调用，URMA分两步 | UrmaAgent.initialize()合并 |
| `nixl_agent.get_agent_metadata()` | UCX EID exchange | `urma_get_jetty_id()` + serialize | 元数据格式不同 | get_agent_metadata()适配格式 |
| `nixl_agent.add_remote_agent(meta)` | UCX remote connect | `urma_import_jetty(ctx, jetty_id)` | 无本质差异 | 直接映射 |
| `nixl_agent.remove_remote_agent()` | UCX disconnect | `urma_unimport_jetty()` | 无差异 | 直接映射 |

## 四、CUDA IPC vs NPU IPC对照

| CUDA IPC API | 英伟达API | NPU IPC API | 差异 | 适配方案 |
|--------------|----------|-------------|------|---------|
| `torch.cuda.Event(interprocess=True)` | cudaEventCreateWithFlags | `torch.npu.Event(interprocess=True)` | API相同，设备不同 | 检测设备类型选择API |
| `event.ipc_handle()` | cudaEventIpcGetHandle | torch_npu IPC handle | 需torch_npu支持 | 等待torch_npu IPC API |
| `torch.cuda.Event.from_ipc_handle()` | cudaEventIpcOpenHandle | torch_npu IPC open | 同上 | 同上 |

## 五、NCCL vs CAM对照

| NCCL API | 英伟达API | CAM API | 差异 | 适配方案 |
|----------|----------|---------|------|---------|
| `ncclSend()` | NCCL点对点发送 | `aclnn_moe_dispatch_normal` | CAM只有MOE算子 | 使用MOE算子模拟Send |
| `ncclRecv()` | NCCL点对点接收 | `aclnn_moe_combine_normal` | 同上 | 使用MOE算子模拟Recv |
| `ncclAllReduce()` | NCCL集合通信 | SyncCollectives (部分) | CAM集合通信不完整 | 扩展CAM算子或使用URMA |

## 六、数据直通对照

| 功能 | 英伟达方案 | UMDK方案 | 差异 | 适配方案 |
|------|----------|---------|------|---------|
| GPU内存RDMA | GDRCopy + nvidia-peermem | URMA DSVA | 实现机制不同 | 使用DSVA flag配置 |
| Token安全 | 无显式Token机制 | URMA_TOKEN (PLAIN/SIGNED/ENCRYPTED) | UMDK更安全 | 配置Token策略 |
| 内存类型 | CUDA统一内存 | 多种mem_type支持 | UMDK更灵活 | 自动检测mem_type |

## 七、快速适配代码示例

### 7.1 内存注册适配

```python
# NIXL调用
handles = nixl_agent.register_memory([{
    'addr': tensor.data_ptr(),
    'size': tensor.nbytes(),
    'gpu_id': tensor.device.index
}])

# URMA适配
from pyurma import UrmaSegCfg, URMA_DSVA_ENABLE, URMA_ACCESS_READ

seg_cfg = UrmaSegCfg()
seg_cfg.va = tensor.data_ptr()
seg_cfg.len = tensor.nbytes()
seg_cfg.dsva = URMA_DSVA_ENABLE if tensor.device.type == 'npu' else 0
seg_cfg.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE

handle = urma_register_seg(ctx, seg_cfg)
```

### 7.2 传输适配

```python
# NIXL调用
xfer_handle = nixl_agent.initialize_xfer(READ, remote_meta)
nixl_agent.transfer(xfer_handle)
while nixl_agent.check_xfer_state(xfer_handle) == "PROC":
    pass

# URMA适配
wr_id = urma_read(jfs, remote_jetty, local_seg, remote_seg, 0, 0, len)
while True:
    crs = urma_poll_jfc(jfc)
    if crs and crs[0].status == URMA_CR_SUCCESS:
        break
```

### 7.3 高级封装

```python
# UrmaAgent封装类 (推荐使用)
agent = UrmaAgent()
agent.initialize()

# 注册内存 (自动处理seg_cfg)
handles = agent.register_memory([tensor1, tensor2])

# 获取描述符
descs = agent.get_xfer_descs(handles)

# 添加远程代理
remote_id = agent.add_remote_agent(remote_meta)

# 执行READ
agent.read(remote_id, handles, [remote_seg_id])

# 检查状态
state = agent.check_xfer_state(handle)
```

---

*快速参考卡片 - 详细文档见 RAY_RDT_NVIDIA_API_vs_UMDK_Analysis.md*