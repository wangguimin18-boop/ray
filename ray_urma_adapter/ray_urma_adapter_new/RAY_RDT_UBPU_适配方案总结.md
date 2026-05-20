# RAY RDT 灵衢总线UBPU数据直通适配方案总结

## 一、核心发现

### 灵衢总线相比英伟达的优势

| 特性 | 英伟达GPUDirect | 华为灵衢总线 | 优势分析 |
|-----|----------------|-------------|---------|
| **统一编址** | 仅GPU间统一编址 | UBVA跨节点统一编址 | 打破节点地址边界 |
| **设备类型** | 仅支持GPU | 支持CPU/NPU/GPU | 全异构计算单元 |
| **跨节点** | 需IB/RoCE + GDRCopy | 原生支持跨节点RDMA | 硬件原生支持 |
| **数据直通** | GDRCopy内核模块 | DSVA原生机制 | 无需额外内核模块 |
| **Kernel Bypass** | 需nvidia-peermem | u-udma原生支持 | 用户态DMA |
| **安全机制** | 无Token机制 | Token策略(PLAIN/SIGNED/ENCRYPTED) | 更安全 |

---

## 二、关键概念

### 2.1 UBPU（Unified Bus Processing Unit）

```
UBPU Function字段结构（48 bits）：
┌───────────────────────────────────────────────────────────┐
│                  Function (48 bits)                       │
├─────────────┬──────────────┬──┬───────────────────────────┤
│UBPU Class   │UBPU Subclass │P │      Method (23b)         │
│  (12 bits)  │  (12 bits)   │1b│                           │
├─────────────┴──────────────┴──┴───────────────────────────┤
│ UBPU Class：指示UBPU类型（CPU/NPU/GPU等）                  │
│ UBPU Subclass：指示UBPU子类型（具体型号）                  │
│ P：Private标志，0=公共函数，1=定制函数                     │
│ Method：具体调用的函数                                     │
└───────────────────────────────────────────────────────────┘
```

**UBPU类型映射**：

| UBPU Class | 处理单元类型 | Ray对应 |
|------------|-------------|---------|
| 0x001 | CPU（通用计算） | CPU Actor |
| 0x002 | NPU（昇腾） | NPU Actor |
| 0x003 | GPU（通用） | GPU Actor |

### 2.2 UBVA（Unified Bus Virtual Address）

```
UBVA结构：
┌───────────────────────────────────────────────────────────┐
│                    UBVA (128 bits)                        │
├─────────────────┬─────────────────┬───────────────────────┤
│    EID (16B)    │    UASID (32b)  │      VA (64b)         │
│   端点标识符    │   用户地址空间ID│     虚拟地址          │
└─────────────────┴─────────────────┴───────────────────────┘

关键能力：
- 跨节点统一编址
- 打破节点地址边界
- 应用通过UBVA直接访问远端内存
```

### 2.3 DSVA（Data Direct Virtual Address）

**数据直通流程**：
```
┌───────┐          ┌───────┐          ┌───────┐
│ UBPU-A│          │灵衢总线│          │ UBPU-B│
│(NPU)  │◄────────►│       │◄────────►│(GPU)  │
└───────┘   DSVA   └───────┘   DSVA   └───────┘
    │                          │
    │ 1.register_seg(DSVA=1)   │
    ├──────────────────────────┤
    │                          │
    │ 2.import_seg             │
    ├──────────────────────────►
    │                          │
    │ 3.urma_read/write        │
    ├──────────────────────────►
    │    直接DMA，无CPU参与     │
```

---

## 三、传输模式

| 模式 | 源UBPU | 目标UBPU | URMA操作 |
|-----|--------|---------|---------|
| IPC_LOCAL | UBPU-NPU | UBPU-NPU(同节点) | urma_import_seg |
| RDMA_DIRECT | UBPU-NPU | UBPU-NPU(跨节点) | urma_read |
| DSVA_CROSS_DEVICE | UBPU-NPU | UBPU-GPU | urma_read(DSVA) |
| DSVA_ONE_SIDED | UBPU-NPU | UBPU-CPU | urma_read |
| RDMA_READ | UBPU-CPU | UBPU-CPU | urma_read |

---

## 四、需要修改的RAY文件

### 4.1 新增文件（6个）

| 文件 | 类 | 功能 |
|------|-----|------|
| `ubpu_info.py` | `UBPUInfo`, `UBPUClass`, `UBPUSubclass` | UBPU类型管理 |
| `ubva_manager.py` | `UBVAManager`, `UBVADescriptor` | UBVA地址管理 |
| `urma_tensor_transport.py` | `UrmaTensorTransport`, `UrmaTransportMetadata` | URMA传输后端 |
| `ubpu_collective_transport.py` | `UBPUCollectiveTransport` | UBPU集合通信 |
| `token_manager.py` | `TokenManager` | Token安全管理 |
| `npu_ipc_transport.py` | `NpuIpcTransport` | NPU IPC传输 |

### 4.2 修改文件（4个）

| 文件 | 修改位置 | 修改内容 |
|------|---------|---------|
| `util.py` | Line 87, 92-114, 268-279 | 添加URMA注册、NPU设备创建 |
| `tensor_transport_manager.py` | Line 17-21, 22-35 | 添加UBPU/UBVA字段 |
| `rdt_manager.py` | Line 42-55, 573-693 | 添加UBPU字段、传输模式判定 |
| `__init__.py` | 导出 | 导出新类 |

### 4.3 详细修改列表

#### util.py

| 位置 | 当前 | 修改后 |
|-----|------|-------|
| Line 87 | `DEFAULT_TRANSPORTS = ["NIXL", "GLOO", "NCCL", "CUDA_IPC"]` | `DEFAULT_TRANSPORTS = ["URMA", "NIXL", ...]` |
| Line 92-114 | `_ensure_default_transports_registered()` | 添加URMA注册逻辑 |
| 新增 | 无 | `_check_urma_available()` |
| 新增 | 无 | `_check_torch_npu_available()` |
| Line 268-279 | `create_empty_tensors_from_metadata()` | 支持NPU设备 |

#### rdt_manager.py

| 位置 | 当前 | 修改后 |
|-----|------|-------|
| Line 42-55 | `RDTMeta` | 添加`src_ubpu_info`, `dst_ubpu_info` |
| Line 573-693 | `trigger_out_of_band_tensor_transfer()` | UBPU传输模式检测 |
| 新增 | 无 | `_get_actor_ubpu()` |
| 新增 | 无 | `_determine_ubpu_transfer_mode()` |

---

## 五、核心类设计

### 5.1 UBPUInfo类

```python
@dataclass
class UBPUInfo:
    ubpu_class: UBPUClass       # CPU/NPU/GPU
    ubpu_subclass: UBPUSubclass # 具体型号
    device_id: int              # 设备ID
    node_eid: bytes             # 节点EID(16B)
    uasid: int                  # 用户地址空间ID
    
    def is_dsva_capable(self) -> bool:
        return self.ubpu_class in [UBPUClass.NPU, UBPUClass.GPU]
    
    def to_function_field(self, method: int) -> int:
        return (self.ubpu_class << 36) | (self.ubpu_subclass << 24) | method
```

### 5.2 UBVAManager类

```python
class UBVAManager:
    def register_memory(self, addr, size, ubpu_info, dsva=True) -> UBVADescriptor:
        # urma_register_seg with DSVA flag
        
    def import_remote_memory(self, descriptor) -> remote_seg:
        # urma_import_seg
        
    def unregister_memory(self, addr):
        # urma_unregister_seg
```

### 5.3 UrmaTensorTransport类

```python
class UrmaTensorTransport(TensorTransportManager):
    def tensor_transport_backend(self) -> str:
        return "URMA"
    
    def recv_multiple_tensors(self, obj_id, metadata, comm_meta, buffers):
        # 1. 解析UBVA描述符
        # 2. 导入远端内存
        # 3. urma_read传输
        # 4. urma_poll_jfc等待完成
    
    def _determine_transfer_mode(self, src_ubpu, dst_ubpu) -> str:
        # 根据UBPU类型判定最优传输模式
```

---

## 六、pyurma绑定设计

### 6.1 模块结构

```
pyurma/
├── __init__.py        # 导出核心API
├── core.py            # urma_init/register_seg/read/write
├── types.py           # urma_seg_t/ubva_t/eid_t
├── agent.py           # UrmaAgent高级封装
├── ubva.py            # UBVA工具
├── ubpu.py            # UBPU工具
├── errors.py          # 错误定义
└── _pyurma.so         # C扩展(pybind11)
```

### 6.2 API映射表

| RAY操作 | NIXL API | URMA API |
|---------|---------|---------|
| 创建代理 | `nixl_agent()` | `urma_init()` + `urma_create_context()` |
| 注册内存 | `register_memory()` | `urma_register_seg()` |
| 生成描述符 | `get_serialized_descs()` | `UBVADescriptor.to_bytes()` |
| 导入远端 | `add_remote_agent()` | `urma_import_seg()` |
| 发起传输 | `transfer()` | `urma_read()` / `urma_write()` |
| 检查状态 | `check_xfer_state()` | `urma_poll_jfc()` |
| 取消注册 | `deregister_memory()` | `urma_unregister_seg()` |

---

## 七、实施路线图

| Phase | 任务 | 时间 | 交付物 |
|-------|------|------|--------|
| **Phase 1** | pyurma Python绑定 | 2周 | `_pyurma.so` |
| **Phase 2** | UBPU抽象层 | 2周 | `ubpu_info.py`, `ubva_manager.py` |
| **Phase 3** | URMA Tensor Transport | 2周 | `urma_tensor_transport.py` |
| **Phase 4** | Ray集成修改 | 1周 | 修改`util.py`, `rdt_manager.py` |
| **Phase 5** | 性能优化与测试 | 2周 | 性能基准、文档 |

**总计：约9周**

---

## 八、使用示例

### 8.1 NPU间传输

```python
import ray
import torch_npu

ray.init(tensor_transport_backend="URMA")

@ray.remote(num_npus=1)
class NPUActor:
    def get_tensor(self):
        return torch.randn(1024, 1024, device="npu:0")

actor1 = NPUActor.remote()
actor2 = NPUActor.remote()

# DSVA直通传输
tensor_ref = actor1.get_tensor.remote()
result = ray.get(actor2.process.remote(tensor_ref))
```

### 8.2 NPU→GPU跨设备

```python
@ray.remote(num_npus=1)
class NPUActor:
    def create_tensor(self):
        return torch.randn(1024, 1024, device="npu:0")

@ray.remote(num_gpus=1)
class GPUActor:
    def process_npu_tensor(self, npu_tensor):
        # DSVA跨设备直通：NPU数据直接访问
        return npu_tensor @ npu_tensor.T

# NPU -> GPU DSVA直通
npu_actor = NPUActor.remote()
gpu_actor = GPUActor.remote()
result = ray.get(gpu_actor.process_npu_tensor.remote(npu_actor.create_tensor.remote()))
```

### 8.3 跨节点传输

```python
@ray.remote(num_npus=1, resources={"node_a": 1})
class NPUActorA:
    def get_tensor(self):
        return torch.randn(10240, 10240, device="npu:0")

@ray.remote(num_npus=1, resources={"node_b": 1})
class NPUActorB:
    def receive(self, tensor):
        return tensor.sum()

# 跨节点UBVA直通
actor_a = NPUActorA.remote()
actor_b = NPUActorB.remote()
result = ray.get(actor_b.receive.remote(actor_a.get_tensor.remote()))
```

---

## 九、文档索引

| 文档 | 路径 |
|------|------|
| **UBPU数据直通方案** | `C:\Users\王贵民\Desktop\ray_urma_adapter\RAY_RDT_UBPU_Direct_Transport_Analysis.md` |
| **UBPU集成指南** | `C:\Users\王贵民\Desktop\ray_urma_adapter\UBPU_Direct_Transport_Integration_Guide.md` |
| **原分析报告** | `C:\Users\王贵民\Desktop\ray_urma_adapter\RAY_RDT_NVIDIA_API_vs_UMDK_Analysis.md` |
| **原集成指南** | `C:\Users\王贵民\Desktop\ray_urma_adapter\integration_guide.md` |
| **API对照表** | `C:\Users\王贵民\Desktop\ray_urma_adapter\API_Quick_Reference.md` |
| **工作总结** | `C:\Users\王贵民\Desktop\ray_urma_adapter\WORK_SUMMARY.md` |

---

## 十、关键头文件路径

| 模块 | 路径 |
|------|------|
| URMA API | `D:\C++\umdk-master\...\src\urma\lib\urma\core\include\urma_api.h` |
| URMA Types | `D:\C++\umdk-master\...\src\urma\lib\urma\core\include\urma_types.h` |
| URPC Message | `D:\C++\umdk-master\...\doc\ch\urpc\URPC Message.ch.md` |
| UDMA Driver | `D:\C++\umdk-master\...\src\urma\hw\udma\README-zh.md` |
| RDT Manager | `D:\C++\ray-ray-2.55.1\...\python\ray\experimental\rdt\rdt_manager.py` |
| Tensor Transport | `D:\C++\ray-ray-2.55.1\...\python\ray\experimental\rdt\tensor_transport_manager.py` |

---

**生成时间: 2025年**

**核心结论**: 灵衢总线通过UBPU统一抽象、UBVA统一编址、DSVA数据直通，实现了比英伟达GPUDirect更强大的跨节点、跨设备数据直通能力。Ray RDT可通过新增UBPU抽象层和URMA传输后端，在灵衢总线超节点集群上实现CPU/NPU/GPU相互数据直通。