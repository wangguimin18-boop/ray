# RAY RDT 基于华为灵衢总线UBPU数据直通能力重构方案

## 一、灵衢总线核心概念

### 1.1 灵衢总线（UnifiedBus）架构

灵衢总线是面向超节点的互联协议，它将各种处理单元之间的I/O、内存访问和通信统一在一个互连技术框架下。相比英伟达的GPUDirect技术，灵衢总线具有更强大的数据直通能力：

| 特性 | 英伟达GPUDirect | 华为灵衢总线 | 优势分析 |
|-----|----------------|-------------|---------|
| **统一编址** | 仅GPU间统一编址 | UBVA跨节点统一编址 | 灵衢打破节点地址边界 |
| **设备类型** | 仅支持GPU | 支持CPU/NPU/GPU | 灵衢支持异构计算单元 |
| **跨节点** | 需要IB/RoCE + GDRCopy | 原生支持跨节点RDMA | 灵衢原生硬件支持 |
| **数据直通** | GDRCopy内核模块 | DSVA原生机制 | 灵衢无需额外内核模块 |
| **Kernel Bypass** | 需要nvidia-peermem | u-udma原生支持 | 灵衢原生用户态驱动 |
| **安全机制** | 无Token机制 | Token策略(PLAIN/SIGNED/ENCRYPTED) | 灵衢更安全 |

### 1.2 UBVA（Unified Bus Virtual Address）

UBVA是灵衢总线上的分级虚拟地址，支持对总线的多个节点共享内存进行统一编址，打破各个节点地址边界，允许应用通过VA进行跨节点寻址和数据访问。

```
UBVA结构：
┌───────────────────────────────────────────────────────────┐
│                    UBVA (128 bits)                        │
├─────────────────┬─────────────────┬───────────────────────┤
│    EID (16B)    │    UASID (32b)  │      VA (64b)         │
│   端点标识符    │   用户地址空间ID│     虚拟地址          │
├─────────────────┴─────────────────┴───────────────────────┤
│ EID包含：IPv4/IPv6地址，标识灵衢总线上的节点               │
│ UASID：用户地址空间标识符，隔离不同进程的地址空间          │
│ VA：64位虚拟地址，支持跨节点寻址                          │
└───────────────────────────────────────────────────────────┘
```

**关键能力**：
- 跨节点统一编址：不同节点的内存通过UBVA统一编址
- 打破地址边界：应用无需关心数据在哪个节点，通过UBVA直接访问
- 支持DSVA：数据直通虚拟地址，允许直接访问设备内存

### 1.3 UBPU（Unified Bus Processing Unit）

UBPU是灵衢总线上的处理单元概念，通过URPC的Function字段识别：

```
UBPU Function字段结构（48 bits）：
┌───────────────────────────────────────────────────────────┐
│                  Function (48 bits)                       │
├─────────────┬──────────────┬──┬───────────────────────────┤
│UBPU Class   │UBPU Subclass │P │      Method (23b)         │
│  (12 bits)  │  (12 bits)   │1b│                           │
├─────────────┴──────────────┴──┴───────────────────────────┤
│ UBPU Class：指示UBPU类型（CPU/NPU/GPU等）                  │
│ UBPU Subclass：指示UBPU子类型（具体型号/架构）             │
│ P：Private标志，0=公共函数，1=定制函数                     │
│ Method：具体调用的函数                                     │
└───────────────────────────────────────────────────────────┘
```

**UBPU类型映射**：

| UBPU Class | UBPU Subclass | 处理单元类型 | Ray对应概念 |
|------------|---------------|-------------|-------------|
| 0x001 | 0x001 | CPU（通用计算） | CPU Actor |
| 0x002 | 0x001 | NPU（昇腾910B） | NPU Actor |
| 0x002 | 0x002 | NPU（昇腾310P） | NPU Actor |
| 0x003 | 0x001 | GPU（通用GPU） | GPU Actor |
| 0x004 | 0x001 | DSP（数字信号处理） | Custom Actor |
| 0x005 | 0x001 | FPGA | Custom Actor |

### 1.4 DSVA（Data Direct Virtual Address）

DSVA是数据直通虚拟地址机制，是实现UBPU间数据直通的核心：

```c
// DSVA配置（来自urma_types.h）
#define URMA_DSVA_DISABLE 0
#define URMA_DSVA_ENABLE  1

// 内存段属性结构
typedef struct urma_seg_attr {
    uint32_t access       : 4;  // 访问权限：READ/WRITE/ATOMIC
    uint32_t token_policy : 2;  // Token策略：PLAIN/SIGNED/ENCRYPTED
    uint32_t cacheability : 2;  // 缓存策略
    uint32_t dsva         : 1;  // DSVA标志：是否启用数据直通
    // ...
} urma_seg_attr_t;

// 内存访问权限
#define URMA_ACCESS_LOCAL_ONLY (0x1 << 0)
#define URMA_ACCESS_READ       (0x1 << 1)
#define URMA_ACCESS_WRITE      (0x1 << 2)
#define URMA_ACCESS_ATOMIC     (0x1 << 3)
```

**DSVA数据直通流程**：

```
┌───────────────────────────────────────────────────────────────┐
│                    DSVA数据直通流程                            │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌───────┐          ┌───────┐          ┌───────┐             │
│  │ UBPU-A│          │灵衢总线│          │ UBPU-B│             │
│  │(NPU)  │          │       │          │(GPU)  │             │
│  └───────┘          └───────┘          └───────┘             │
│     │                  │                  │                  │
│     │ 1.register_seg   │                  │                  │
│     │ (DSVA=ENABLE)    │                  │                  │
│     ├─────────────────►│                  │                  │
│     │                  │ 2.分配UBVA       │                  │
│     │◄─────────────────┤                  │                  │
│     │                  │                  │                  │
│     │                  │ 3.import_seg     │                  │
│     │                  ├─────────────────►│                  │
│     │                  │                  │ 4.映射到本地     │
│     │                  │◄─────────────────┤                  │
│     │                  │                  │                  │
│     │ 5.urma_read/write│                  │                  │
│     ├─────────────────►│─────────────────►│                  │
│     │    (直接DMA)     │   无CPU参与      │                  │
│     │                  │                  │                  │
│     │                  │ 6.poll_jfc       │                  │
│     │◄─────────────────┤◄─────────────────┤                  │
│     │    完成通知      │                  │                  │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

---

## 二、RAY RDT架构分析与重构需求

### 2.1 现有RAY RDT架构

RAY RDT（Ray Direct Transport）是一种GPU/NPU数据直通传输机制，当前架构：

```
┌───────────────────────────────────────────────────────────────┐
│                    现有RAY RDT架构                             │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────┐                                          │
│  │  TensorTransport│  抽象基类                                │
│  │    Manager      │                                          │
│  └───────┬─────────┘                                          │
│          │                                                    │
│    ┌─────┴─────┬─────────────┬─────────────┐                 │
│    │           │             │             │                 │
│ ┌──▼───┐  ┌───▼───┐  ┌──────▼──┐  ┌──────▼──┐               │
│ │NIXL  │  │NCCL   │  │GLOO    │  │CUDA_IPC │               │
│ │(GPU) │  │(GPU)  │  │(CPU)   │  │(同节点) │               │
│ └──────┘  └───────┘  └─────────┘  └─────────┘               │
│                                                               │
│ 问题：                                                        │
│ 1. 仅支持GPU，不支持NPU/异构设备                              │
│ 2. 跨设备传输需要CPU参与（NCCL双边操作）                       │
│ 3. 跨节点需要额外的IB/RoCE配置                                │
│ 4. 无统一编址，需要手动管理地址                                │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### 2.2 核心类和方法列表

#### 2.2.1 `TensorTransportManager`（抽象基类）

**文件位置**: `python/ray/experimental/rdt/tensor_transport_manager.py`

| 方法 | 功能 | 需要修改 |
|-----|------|---------|
| `tensor_transport_backend()` | 返回后端名称 | 新增"URMA"后端 |
| `is_one_sided()` | 是否单边传输 | URMA支持单边 |
| `can_abort_transport()` | 是否支持abort | URMA支持abort |
| `actor_has_tensor_transport()` | 检查Actor能力 | 支持NPU检测 |
| `extract_tensor_transport_metadata()` | 提取传输元数据 | 改用UBVA |
| `get_communicator_metadata()` | 获取通信元数据 | 支持UBPU映射 |
| `recv_multiple_tensors()` | 接收张量 | 使用URMA READ |
| `send_multiple_tensors()` | 发送张量 | 使用URMA WRITE |
| `garbage_collect()` | 垃圾回收 | URMA unregister |
| `abort_transport()` | 中止传输 | URMA abort |

#### 2.2.2 `NixlTensorTransport`（NIXL后端）

**文件位置**: `python/ray/experimental/rdt/nixl_tensor_transport.py`

| 方法 | 当前实现 | URMA重构 |
|-----|---------|---------|
| `get_nixl_agent()` | 创建NIXL代理 | 改为`get_urma_context()` |
| `_add_tensor_descs()` | NIXL内存注册 | 改为`urma_register_seg()` |
| `extract_tensor_transport_metadata()` | NIXL描述符 | 改为UBVA描述 |
| `recv_multiple_tensors()` | NIXL READ传输 | 改为URMA READ |
| `garbage_collect()` | NIXL deregister | 改为`urma_unregister_seg()` |

#### 2.2.3 `util.py`（工具函数）

**文件位置**: `python/ray/experimental/rdt/util.py`

| 函数/变量 | 当前值 | 需要修改 |
|----------|-------|---------|
| `DEFAULT_TRANSPORTS` | ["NIXL", "GLOO", "NCCL", "CUDA_IPC"] | 添加"URMA"优先 |
| `register_tensor_transport()` | 注册传输后端 | 添加URMA注册 |
| `_ensure_default_transports_registered()` | 注册默认传输 | 添加URMA检测 |
| `get_tensor_transport_manager()` | 获取传输管理器 | 支持URMA |
| `create_empty_tensors_from_metadata()` | 创建空张量 | 支持NPU设备 |

#### 2.2.4 `RDTManager`（RDT管理器）

**文件位置**: `python/ray/experimental/rdt/rdt_manager.py`

| 方法 | 当前实现 | 需要修改 |
|-----|---------|---------|
| `add_rdt_ref()` | 添加RDT引用 | 支持UBPU类型标记 |
| `trigger_out_of_band_tensor_transfer()` | 触发传输 | UBPU间直通检测 |
| `_abort_transport()` | 中止传输 | URMA abort |
| `_fetch_object()` | 获取对象 | UBVA寻址 |

---

## 三、基于灵衢总线的UBPU数据直通重构方案

### 3.1 新架构设计

```
┌───────────────────────────────────────────────────────────────────┐
│                  新架构：基于灵衢总线的UBPU数据直通                │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                UBPU Tensor Transport Layer                  │ │
│  │                  (统一抽象层)                                │ │
│  ├─────────────────────────────────────────────────────────────┤ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │ │
│  │  │UBPU-Info │  │ UBVA-Mgr │  │ DSVA-Mgr │  │Token-Mgr │    │ │
│  │  │(类型管理)│  │(地址管理)│  │(直通管理)│  │(安全管理)│    │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                              │                                    │
│                              ▼                                    │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                   URMA Transport Backend                    │ │
│  ├───────────────────┬───────────────────┬─────────────────────┤ │
│  │   URMA Memory     │   URMA Transfer   │   URMA Completion   │ │
│  │     Manager       │     Manager       │      Manager        │ │
│  ├───────────────────┼───────────────────┼─────────────────────┤ │
│  │ urma_register_seg │ urma_read/write   │ urma_poll_jfc       │ │
│  │ urma_import_seg   │ urma_send/recv    │ urma_wait_jfc       │ │
│  │ urma_unregister   │ urma_post_wr      │ urma_rearm_jfc      │ │
│  └───────────────────┴───────────────────┴─────────────────────┤ │
│  └─────────────────────────────────────────────────────────────┘ │
│                              │                                    │
│                              ▼                                    │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                  灵衢总线硬件层                              │ │
│  ├──────────────┬──────────────┬──────────────┬────────────────┤ │
│  │    UDMA      │    UMMU      │    UBCore    │    URPC        │ │
│  │  (DMA引擎)   │ (内存管理)   │ (核心协议)   │ (RPC加速)      │ │
│  ├──────────────┼──────────────┼──────────────┼────────────────┤ │
│  │Kernel Bypass │ 地址翻译     │ UB协议栈     │ UBPU通信       │ │
│  │ 用户态DMA    │ TPA映射      │ 跨节点通信   │ Function调用   │ │
│  └──────────────┴──────────────┴──────────────┴────────────────┤ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  ┌───────┐          ┌───────┐          ┌───────┐                 │
│  │ UBPU-A│◄────────►│ UBPU-B│◄────────►│ UBPU-C│                 │
│  │(NPU)  │   DSVA   │(GPU)  │   DSVA   │(CPU)  │                 │
│  └───────┘  直通    └───────┘  直通    └───────┘                 │
│                                                                   │
│  关键特性：                                                       │
│  1. CPU/NPU/GPU统一抽象为UBPU                                    │
│  2. UBVA跨节点统一编址                                           │
│  3. DSVA实现设备间数据直通                                        │
│  4. 无CPU参与的单边传输                                          │
│  5. Token安全机制                                                │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

### 3.2 新增类设计

#### 3.2.1 `UBPUInfo`类（处理单元信息）

**文件**: `python/ray/experimental/rdt/ubpu_info.py`

```python
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

class UBPUClass(IntEnum):
    CPU = 0x001
    NPU = 0x002
    GPU = 0x003
    DSP = 0x004
    FPGA = 0x005

class UBPUSubclass(IntEnum):
    CPU_GENERAL = 0x001
    NPU_ASCEND_910B = 0x001
    NPU_ASCEND_310P = 0x002
    GPU_GENERAL = 0x001

@dataclass
class UBPUInfo:
    ubpu_class: UBPUClass
    ubpu_subclass: UBPUSubclass
    device_id: int
    node_eid: bytes
    uasid: int
    
    def to_function_field(self, method: int, private: bool = False) -> int:
        p_flag = 1 if private else 0
        return (self.ubpu_class << 36) | (self.ubpu_subclass << 24) | (p_flag << 23) | method
    
    def is_dsva_capable(self) -> bool:
        return self.ubpu_class in [UBPUClass.NPU, UBPUClass.GPU]
    
    @staticmethod
    def from_torch_device(device) -> 'UBPUInfo':
        import torch
        if device.type == "npu":
            return UBPUInfo(UBPUClass.NPU, UBPUSubclass.NPU_ASCEND_910B, device.index, ...)
        elif device.type == "cuda":
            return UBPUInfo(UBPUClass.GPU, UBPUSubclass.GPU_GENERAL, device.index, ...)
        else:
            return UBPUInfo(UBPUClass.CPU, UBPUSubclass.CPU_GENERAL, -1, ...)
```

#### 3.2.2 `UBVAManager`类（统一地址管理）

**文件**: `python/ray/experimental/rdt/ubva_manager.py`

```python
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import threading

@dataclass
class UBVADescriptor:
    eid: bytes
    uasid: int
    va: int
    size: int
    dsva_enabled: bool
    token_id: int
    
    def to_bytes(self) -> bytes:
        return self._serialize()
    
    @staticmethod
    def from_bytes(data: bytes) -> 'UBVADescriptor':
        return UBVADescriptor._deserialize(data)

class UBVAManager:
    def __init__(self, urma_context):
        self._context = urma_context
        self._seg_cache: Dict[int, UBVADescriptor] = {}
        self._lock = threading.RLock()
        
    def register_memory(self, addr: int, size: int, ubpu_info: UBPUInfo, 
                        dsva: bool = True) -> UBVADescriptor:
        from pyurma import urma_register_seg, urma_seg_cfg_t, URMA_DSVA_ENABLE
        
        seg_cfg = urma_seg_cfg_t()
        seg_cfg.va = addr
        seg_cfg.len = size
        seg_cfg.flag.bs.dsva = URMA_DSVA_ENABLE if dsva else URMA_DSVA_DISABLE
        seg_cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE
        
        seg = urma_register_seg(self._context, seg_cfg)
        
        descriptor = UBVADescriptor(
            eid=seg.ubva.eid.raw,
            uasid=seg.ubva.uasid,
            va=seg.ubva.va,
            size=size,
            dsva_enabled=dsva,
            token_id=seg.token_id
        )
        
        with self._lock:
            self._seg_cache[addr] = descriptor
        
        return descriptor
    
    def import_remote_memory(self, descriptor: UBVADescriptor) -> 'urma_target_seg_t':
        from pyurma import urma_import_seg
        
        seg_cfg = urma_seg_cfg_t()
        seg_cfg.ubva.eid.raw = descriptor.eid
        seg_cfg.ubva.uasid = descriptor.uasid
        seg_cfg.ubva.va = descriptor.va
        
        return urma_import_seg(self._context, seg_cfg)
    
    def unregister_memory(self, addr: int):
        from pyurma import urma_unregister_seg
        with self._lock:
            if addr in self._seg_cache:
                urma_unregister_seg(self._seg_cache[addr].seg_handle)
                self._seg_cache.pop(addr)
```

#### 3.2.3 `UrmaTensorTransport`类（URMA传输后端）

**文件**: `python/ray/experimental/rdt/urma_tensor_transport.py`

```python
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)
from ray.experimental.rdt.ubpu_info import UBPUInfo, UBPUClass
from ray.experimental.rdt.ubva_manager import UBVAManager, UBVADescriptor

if TYPE_CHECKING:
    import torch

@dataclass
class UrmaCommunicatorMetadata(CommunicatorMetadata):
    src_ubpu: Optional[UBPUInfo] = None
    dst_ubpu: Optional[UBPUInfo] = None
    transfer_mode: str = "RDMA_READ"

@dataclass
class UrmaTransportMetadata(TensorTransportMetadata):
    ubva_descriptors: Optional[List[bytes]] = None
    src_ubpu_info: Optional[bytes] = None
    urma_agent_eid: Optional[bytes] = None
    urma_agent_uasid: Optional[int] = None
    dsva_enabled: bool = True
    token_policy: int = 0

class UrmaTensorTransport(TensorTransportManager):
    def __init__(self):
        self._urma_ctx = None
        self._urma_jfs = None
        self._urma_jfc = None
        self._ubva_manager: Optional[UBVAManager] = None
        self._remote_jettys: OrderedDict = OrderedDict()
        self._aborted_transfer_obj_ids = set()
        self._aborted_lock = threading.Lock()
        self._cache_lock = threading.RLock()
        
    def tensor_transport_backend(self) -> str:
        return "URMA"
    
    @staticmethod
    def is_one_sided() -> bool:
        return True
    
    @staticmethod
    def can_abort_transport() -> bool:
        return True
    
    def get_urma_context(self):
        if self._urma_ctx is not None:
            return self._urma_ctx
            
        from pyurma import (
            urma_init, urma_get_device_list, urma_create_context,
            urma_create_jetty, urma_jetty_cfg_t
        )
        
        urma_init(None)
        devices = urma_get_device_list()
        if len(devices) > 0:
            self._urma_ctx = urma_create_context(devices[0], 0)
            
            jetty_cfg = urma_jetty_cfg_t()
            jetty_cfg.max_jfs = 256
            jetty_cfg.max_jfr = 256
            jetty_cfg.jfc_size = 4096
            self._urma_jetty = urma_create_jetty(self._urma_ctx, jetty_cfg)
            self._urma_jfs = self._urma_jetty.jfs
            self._urma_jfc = self._urma_jetty.jfc
            
            self._ubva_manager = UBVAManager(self._urma_ctx)
            
        return self._urma_ctx
    
    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        def __check_urma__(self):
            try:
                from ray.experimental.rdt.util import get_tensor_transport_manager
                mgr = get_tensor_transport_manager("URMA")
                mgr.get_urma_context()
                return True
            except:
                return False
        
        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(__check_urma__)
        )
    
    def _detect_ubpu_type(self, tensors: List["torch.Tensor"]) -> UBPUInfo:
        import torch
        import torch_npu
        
        device = tensors[0].device
        ctx = ray.get_runtime_context()
        node_id = ctx.get_node_id()
        
        if device.type == "npu":
            return UBPUInfo(
                ubpu_class=UBPUClass.NPU,
                ubpu_subclass=UBPUSubclass.NPU_ASCEND_910B,
                device_id=device.index,
                node_eid=self._get_node_eid(),
                uasid=self._urma_ctx.uasid if self._urma_ctx else 0
            )
        elif device.type == "cuda":
            return UBPUInfo(
                ubpu_class=UBPUClass.GPU,
                ubpu_subclass=UBPUSubclass.GPU_GENERAL,
                device_id=device.index,
                node_eid=self._get_node_eid(),
                uasid=self._urma_ctx.uasid if self._urma_ctx else 0
            )
        else:
            return UBPUInfo(
                ubpu_class=UBPUClass.CPU,
                ubpu_subclass=UBPUSubclass.CPU_GENERAL,
                device_id=-1,
                node_eid=self._get_node_eid(),
                uasid=self._urma_ctx.uasid if self._urma_ctx else 0
            )
    
    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List["torch.Tensor"],
    ) -> UrmaTransportMetadata:
        import torch
        import torch_npu
        
        with self._cache_lock:
            device = None
            tensor_meta = []
            ubva_descriptors = []
            
            if rdt_object:
                device = rdt_object[0].device
                ubpu_info = self._detect_ubpu_type(rdt_object)
                
                for t in rdt_object:
                    if t.device.type != device.type:
                        raise ValueError("All tensors must have same device type")
                    if not t.is_contiguous():
                        raise ValueError("All tensors must be contiguous")
                    tensor_meta.append((t.shape, t.dtype))
                
                if device.type in ["npu", "cuda"]:
                    for dev in set(t.device for t in rdt_object):
                        if device.type == "npu":
                            torch_npu.npu.synchronize(dev)
                        else:
                            torch.cuda.synchronize(dev)
                
                urma_ctx = self.get_urma_context()
                
                for tensor in rdt_object:
                    descriptor = self._ubva_manager.register_memory(
                        addr=tensor.untyped_storage().data_ptr(),
                        size=tensor.untyped_storage().nbytes(),
                        ubpu_info=ubpu_info,
                        dsva=ubpu_info.is_dsva_capable()
                    )
                    ubva_descriptors.append(descriptor.to_bytes())
                
            return UrmaTransportMetadata(
                tensor_meta=tensor_meta,
                tensor_device=device.type if device else None,
                ubva_descriptors=ubva_descriptors if rdt_object else None,
                src_ubpu_info=ubpu_info.to_bytes() if rdt_object else None,
                urma_agent_eid=urma_ctx.eid.raw if urma_ctx else None,
                urma_agent_uasid=urma_ctx.uasid if urma_ctx else None,
                dsva_enabled=ubpu_info.is_dsva_capable() if rdt_object else False,
            )
    
    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> UrmaCommunicatorMetadata:
        src_ubpu = self._get_actor_ubpu(src_actor)
        dst_ubpu = self._get_actor_ubpu(dst_actor)
        
        transfer_mode = self._determine_transfer_mode(src_ubpu, dst_ubpu)
        
        return UrmaCommunicatorMetadata(
            src_ubpu=src_ubpu,
            dst_ubpu=dst_ubpu,
            transfer_mode=transfer_mode
        )
    
    def _determine_transfer_mode(self, src_ubpu: UBPUInfo, dst_ubpu: UBPUInfo) -> str:
        if src_ubpu.ubpu_class == dst_ubpu.ubpu_class:
            if src_ubpu.node_eid == dst_ubpu.node_eid:
                return "IPC_LOCAL"
            else:
                return "RDMA_DIRECT"
        else:
            if src_ubpu.is_dsva_capable() and dst_ubpu.is_dsva_capable():
                return "DSVA_CROSS_DEVICE"
            elif src_ubpu.is_dsva_capable() or dst_ubpu.is_dsva_capable():
                return "DSVA_ONE_SIDED"
            else:
                return "RDMA_READ"
    
    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> List["torch.Tensor"]:
        from ray.experimental.rdt.util import create_empty_tensors_from_metadata
        from pyurma import (
            urma_import_jetty, urma_import_seg, urma_read,
            urma_poll_jfc, URMA_CR_SUCCESS
        )
        
        tensors = target_buffers or create_empty_tensors_from_metadata(tensor_transport_metadata)
        
        assert isinstance(tensor_transport_metadata, UrmaTransportMetadata)
        assert isinstance(communicator_metadata, UrmaCommunicatorMetadata)
        
        with self._aborted_lock:
            if obj_id in self._aborted_transfer_obj_ids:
                self._aborted_transfer_obj_ids.remove(obj_id)
                raise RuntimeError(f"URMA transfer aborted for {obj_id}")
        
        if not tensors:
            return []
        
        urma_ctx = self.get_urma_context()
        
        remote_descriptors = [
            UBVADescriptor.from_bytes(d) 
            for d in tensor_transport_metadata.ubva_descriptors
        ]
        
        dst_ubpu = self._detect_ubpu_type(tensors)
        
        local_descriptors = []
        for tensor in tensors:
            desc = self._ubva_manager.register_memory(
                addr=tensor.untyped_storage().data_ptr(),
                size=tensor.untyped_storage().nbytes(),
                ubpu_info=dst_ubpu,
                dsva=dst_ubpu.is_dsva_capable()
            )
            local_descriptors.append(desc)
        
        remote_jetty_id = tensor_transport_metadata.urma_agent_eid
        
        transfer_mode = communicator_metadata.transfer_mode
        
        try:
            if transfer_mode in ["RDMA_READ", "DSVA_CROSS_DEVICE", "DSVA_ONE_SIDED"]:
                for i, (local_desc, remote_desc) in enumerate(zip(local_descriptors, remote_descriptors)):
                    remote_seg = self._ubva_manager.import_remote_memory(remote_desc)
                    
                    urma_read(
                        self._urma_jfs,
                        remote_jetty_id,
                        local_desc.seg_handle,
                        remote_seg,
                        local_desc.va,
                        remote_desc.va,
                        remote_desc.size,
                        0
                    )
                
                while True:
                    cr_count = urma_poll_jfc(self._urma_jfc, 1)
                    if cr_count > 0:
                        if cr_count[0].status == URMA_CR_SUCCESS:
                            break
                        else:
                            raise RuntimeError(f"URMA transfer failed: {cr_count[0].status}")
                    
                    with self._aborted_lock:
                        if obj_id in self._aborted_transfer_obj_ids:
                            raise RuntimeError(f"URMA transfer aborted for {obj_id}")
                    
                    time.sleep(0.001)
                    
        finally:
            with self._aborted_lock:
                self._aborted_transfer_obj_ids.discard(obj_id)
            
            for desc in local_descriptors:
                self._ubva_manager.unregister_memory(desc.va)
        
        return tensors
    
    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError("URMA is one-sided transport")
    
    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List["torch.Tensor"],
    ):
        with self._cache_lock:
            for tensor in tensors:
                addr = tensor.untyped_storage().data_ptr()
                self._ubva_manager.unregister_memory(addr)
    
    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        with self._aborted_lock:
            self._aborted_transfer_obj_ids.add(obj_id)
```

#### 3.2.4 `UBPUCollectiveTransport`类（UBPU集合通信）

**文件**: `python/ray/experimental/rdt/ubpu_collective_transport.py`

```python
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)
from ray.experimental.rdt.ubpu_info import UBPUInfo

if TYPE_CHECKING:
    import torch

@dataclass
class UBPUCollectiveMetadata(CommunicatorMetadata):
    ubpu_group_id: str = ""
    ubpu_group_members: List[bytes] = []
    src_ubpu_rank: int = 0
    dst_ubpu_rank: int = 0
    collective_type: str = "SEND_RECV"

class UBPUCollectiveTransport(TensorTransportManager):
    def __init__(self):
        self._ubpu_groups: Dict[str, List[UBPUInfo]] = {}
        
    def tensor_transport_backend(self) -> str:
        return "UBPU_COLLECTIVE"
    
    @staticmethod
    def is_one_sided() -> bool:
        return False
    
    @staticmethod
    def can_abort_transport() -> bool:
        return False
    
    def create_ubpu_group(self, actors: List["ray.actor.ActorHandle"], 
                          group_name: str) -> UBPUCollectiveMetadata:
        ubpu_infos = [self._get_actor_ubpu(a) for a in actors]
        self._ubpu_groups[group_name] = ubpu_infos
        
        return UBPUCollectiveMetadata(
            ubpu_group_id=group_name,
            ubpu_group_members=[u.to_bytes() for u in ubpu_infos],
            src_ubpu_rank=0,
            dst_ubpu_rank=0
        )
    
    def recv_multiple_tensors(self, obj_id, metadata, comm_meta, target_buffers=None):
        from cam import SyncCollectives
        
        sync = SyncCollectives()
        sync.Init(comm_meta.src_ubpu_rank, len(comm_meta.ubpu_group_members), ...)
        sync.WaitSyncFlag(...)
        
        return target_buffers
    
    def send_multiple_tensors(self, tensors, metadata, comm_meta):
        from cam import SyncCollectives
        
        sync = SyncCollectives()
        sync.Init(comm_meta.dst_ubpu_rank, len(comm_meta.ubpu_group_members), ...)
        sync.SetSyncFlag(...)
```

---

## 四、需要修改的RAY文件详细列表

### 4.1 新增文件

| 文件路径 | 类/函数 | 功能说明 |
|---------|--------|---------|
| `python/ray/experimental/rdt/ubpu_info.py` | `UBPUInfo`, `UBPUClass`, `UBPUSubclass` | UBPU类型信息管理 |
| `python/ray/experimental/rdt/ubva_manager.py` | `UBVAManager`, `UBVADescriptor` | UBVA地址管理 |
| `python/ray/experimental/rdt/urma_tensor_transport.py` | `UrmaTensorTransport`, `UrmaTransportMetadata` | URMA传输后端 |
| `python/ray/experimental/rdt/ubpu_collective_transport.py` | `UBPUCollectiveTransport` | UBPU集合通信 |
| `python/ray/experimental/rdt/token_manager.py` | `TokenManager` | Token安全管理 |

### 4.2 修改文件

#### 4.2.1 `util.py`修改详情

**文件**: `python/ray/experimental/rdt/util.py`

| 修改位置 | 当前代码 | 修改后代码 | 说明 |
|---------|---------|-----------|------|
| Line 87 | `DEFAULT_TRANSPORTS = ["NIXL", "GLOO", "NCCL", "CUDA_IPC"]` | `DEFAULT_TRANSPORTS = ["URMA", "NIXL", "GLOO", "NCCL", "CUDA_IPC"]` | 添加URMA优先 |
| Line 92-114 | `_ensure_default_transports_registered()` | 添加URMA注册逻辑 | 注册URMA后端 |
| 新增函数 | 无 | `_check_urma_available()` | 检测URMA可用 |
| 新增函数 | 无 | `_check_torch_npu_available()` | 检测NPU可用 |
| 新增函数 | 无 | `_detect_ubpu_type()` | 检测UBPU类型 |
| Line 268-279 | `create_empty_tensors_from_metadata()` | 支持NPU设备创建 | 添加npu设备支持 |

**具体修改代码**：

```python
DEFAULT_TRANSPORTS = ["URMA", "NIXL", "GLOO", "NCCL", "CUDA_IPC"]

def _check_urma_available() -> bool:
    try:
        from pyurma import urma_init
        urma_init(None)
        return True
    except ImportError:
        return False

def _check_torch_npu_available() -> bool:
    try:
        import torch_npu
        return torch.npu.is_available()
    except ImportError:
        return False

def _ensure_default_transports_registered():
    global _default_transports_registered
    with transport_managers_lock:
        if _default_transports_registered:
            return
        _default_transports_registered = True
        try:
            import torch
            
            if _check_urma_available():
                from ray.experimental.rdt.urma_tensor_transport import UrmaTensorTransport
                register_tensor_transport(
                    "URMA", ["npu", "cuda", "cpu"], UrmaTensorTransport, torch.Tensor
                )
            
            register_tensor_transport(
                "NIXL", ["cuda", "cpu"], NixlTensorTransport, torch.Tensor
            )
            register_tensor_transport(
                "GLOO", ["cpu"], GLOOTensorTransport, torch.Tensor
            )
            register_tensor_transport(
                "NCCL", ["cuda"], NCCLTensorTransport, torch.Tensor
            )
            register_tensor_transport(
                "CUDA_IPC", ["cuda"], CudaIpcTransport, torch.Tensor
            )
            
            if _check_torch_npu_available():
                from ray.experimental.rdt.npu_ipc_transport import NpuIpcTransport
                register_tensor_transport(
                    "NPU_IPC", ["npu"], NpuIpcTransport, torch.Tensor
                )
        except ImportError:
            pass

def create_empty_tensors_from_metadata(
    tensor_transport_meta: TensorTransportMetadata,
) -> List["torch.Tensor"]:
    import torch
    import torch_npu
    
    tensors = []
    device = tensor_transport_meta.tensor_device
    
    for meta in tensor_transport_meta.tensor_meta:
        shape, dtype = meta
        
        if device == "npu":
            tensor = torch.empty(shape, dtype=dtype, device=f"npu:{torch.npu.current_device()}")
        elif device == "cuda":
            tensor = torch.empty(shape, dtype=dtype, device=device)
        else:
            tensor = torch.empty(shape, dtype=dtype, device="cpu")
        
        tensors.append(tensor)
    
    return tensors
```

#### 4.2.2 `tensor_transport_manager.py`修改详情

**文件**: `python/ray/experimental/rdt/tensor_transport_manager.py`

| 修改位置 | 当前内容 | 修改内容 | 说明 |
|---------|---------|---------|------|
| Line 17-21 | `CommunicatorMetadata` | 添加`UBPUInfo`支持 | 支持UBPU类型 |
| Line 22-35 | `TensorTransportMetadata` | 添加UBVA字段 | 支持UBVA描述 |
| Line 37-185 | `TensorTransportManager` | 无需修改 | 抽象类保持不变 |

#### 4.2.3 `rdt_manager.py`修改详情

**文件**: `python/ray/experimental/rdt/rdt_manager.py`

| 修改位置 | 当前代码 | 修改内容 | 说明 |
|---------|---------|---------|------|
| Line 42-55 | `RDTMeta` | 添加`ubpu_info`字段 | 记录UBPU类型 |
| Line 293-373 | `_abort_transport()` | 添加URMA abort逻辑 | URMA中止支持 |
| Line 573-693 | `trigger_out_of_band_tensor_transfer()` | UBPU直通检测 | 检测跨UBPU传输 |
| Line 447-540 | `_fetch_object()` | UBVA寻址 | 支持UBVA获取 |

**具体修改代码**：

```python
class RDTMeta(NamedTuple):
    src_actor: "ray.actor.ActorHandle"
    tensor_transport_backend: str
    tensor_transport_meta: Optional["TensorTransportMetadata"]
    sent_dest_actors: Set[str]
    sent_to_src_actor_and_others_warned: bool
    target_buffers: Optional[List[weakref.ReferenceType[Any]]]
    src_ubpu_info: Optional[bytes]  # 新增：源UBPU信息
    dst_ubpu_info: Optional[bytes]  # 新增：目标UBPU信息

def trigger_out_of_band_tensor_transfer(self, dst_actor, obj_id):
    from ray.experimental.rdt.ubpu_info import UBPUInfo
    
    rdt_meta = self._managed_rdt_metadata[obj_id]
    src_actor = rdt_meta.src_actor
    
    src_ubpu = UBPUInfo.from_bytes(rdt_meta.src_ubpu_info) if rdt_meta.src_ubpu_info else None
    dst_ubpu = self._get_actor_ubpu(dst_actor)
    
    if src_ubpu and dst_ubpu:
        if src_ubpu.node_eid == dst_ubpu.node_eid:
            if src_ubpu.ubpu_class == dst_ubpu.ubpu_class:
                tensor_transport = "URMA_IPC"
            else:
                tensor_transport = "URMA_DSVA"
        else:
            tensor_transport = "URMA_RDMA"
    else:
        tensor_transport = rdt_meta.tensor_transport_backend
    
    tensor_transport_manager = get_tensor_transport_manager(tensor_transport)
    communicator_meta = tensor_transport_manager.get_communicator_metadata(
        src_actor, dst_actor, tensor_transport
    )
```

#### 4.2.4 `nixl_tensor_transport.py`修改详情

**文件**: `python/ray/experimental/rdt/nixl_tensor_transport.py`

| 修改位置 | 当前实现 | 修改内容 | 说明 |
|---------|---------|---------|------|
| Line 51-71 | `NixlTensorTransport.__init__` | 添加UBPU检测 | 支持设备类型检测 |
| Line 129-182 | `extract_tensor_transport_metadata()` | 添加UBVA生成 | 支持UBVA描述 |
| Line 192-313 | `recv_multiple_tensors()` | URMA API映射 | 使用URMA传输 |

---

## 五、pyurma Python绑定详细设计

### 5.1 模块结构

```
pyurma/
├── __init__.py              # 模块入口
├── core.py                  # 核心API绑定
├── types.py                 # 数据类型定义
├── agent.py                 # UrmaAgent高级封装
├── ubva.py                  # UBVA管理
├── ubpu.py                  # UBPU管理
├── errors.py                # 错误定义
├── utils.py                 # 工具函数
└── _pyurma.so               # C扩展模块
```

### 5.2 核心API绑定

**文件**: `pyurma/core.py`

```python
from typing import List, Optional, Tuple
from ._pyurma import (
    urma_init as _urma_init,
    urma_uninit as _urma_uninit,
    urma_get_device_list as _urma_get_device_list,
    urma_create_context as _urma_create_context,
    urma_delete_context as _urma_delete_context,
    urma_register_seg as _urma_register_seg,
    urma_unregister_seg as _urma_unregister_seg,
    urma_import_seg as _urma_import_seg,
    urma_unimport_seg as _urma_unimport_seg,
    urma_create_jetty as _urma_create_jetty,
    urma_create_jfs as _urma_create_jfs,
    urma_create_jfc as _urma_create_jfc,
    urma_read as _urma_read,
    urma_write as _urma_write,
    urma_send as _urma_send,
    urma_recv as _urma_recv,
    urma_poll_jfc as _urma_poll_jfc,
    urma_wait_jfc as _urma_wait_jfc,
)

def urma_init(config: Optional[dict] = None) -> int:
    if config is None:
        return _urma_init(None)
    init_attr = urma_init_attr_t()
    init_attr.uasid = config.get('uasid', 0)
    init_attr.token = config.get('token', 0)
    return _urma_init(init_attr)

def urma_register_seg(ctx, seg_cfg) -> 'urma_seg_t':
    return _urma_register_seg(ctx, seg_cfg)

def urma_read(jfs, remote_jetty, local_seg, remote_seg, 
              dst_addr, src_addr, length, flags) -> int:
    return _urma_read(jfs, remote_jetty, local_seg, remote_seg,
                      dst_addr, src_addr, length, flags)
```

### 5.3 数据类型定义

**文件**: `pyurma/types.py`

```python
from dataclasses import dataclass
from typing import Union

@dataclass
class urma_eid_t:
    raw: bytes
    
    @property
    def ipv4(self) -> int:
        return int.from_bytes(self.raw[12:16], 'big')
    
    @staticmethod
    def from_ipv4(ipv4: int) -> 'urma_eid_t':
        raw = bytes(12) + bytes([0x00, 0x00, 0xff, 0xff]) + ipv4.to_bytes(4, 'big')
        return urma_eid_t(raw)

@dataclass
class urma_ubva_t:
    eid: urma_eid_t
    uasid: int
    va: int

@dataclass
class urma_seg_t:
    ubva: urma_ubva_t
    token_id: int
    size: int
    attr: urma_seg_attr_t

@dataclass
class urma_seg_cfg_t:
    va: int
    len: int
    flag: urma_seg_flag_t
    ubva: Optional[urma_ubva_t] = None

@dataclass
class urma_seg_attr_t:
    access: int
    token_policy: int
    dsva: int
    cacheability: int

@dataclass
class urma_cr_t:
    status: int
    length: int
    user_ctx: int
    notify_data: int

URMA_ACCESS_READ = 0x2
URMA_ACCESS_WRITE = 0x4
URMA_ACCESS_ATOMIC = 0x8
URMA_DSVA_ENABLE = 1
URMA_DSVA_DISABLE = 0
URMA_CR_SUCCESS = 0
```

### 5.4 UrmaAgent高级封装

**文件**: `pyurma/agent.py`

```python
from typing import List, Optional, Dict
import threading
from .core import urma_init, urma_create_context, urma_register_seg
from .types import urma_seg_cfg_t, urma_ubva_t, URMA_ACCESS_READ, URMA_ACCESS_WRITE

class UrmaAgent:
    def __init__(self, name: str, config: Optional[dict] = None):
        self._name = name
        self._ctx = None
        self._jfs = None
        self._jfc = None
        self._seg_cache: Dict[int, urma_seg_t] = {}
        self._lock = threading.RLock()
        self._config = config or {}
        
    def initialize(self) -> bool:
        urma_init(self._config)
        
        devices = urma_get_device_list()
        if len(devices) == 0:
            return False
        
        self._ctx = urma_create_context(devices[0], 0)
        
        jetty_cfg = urma_jetty_cfg_t()
        jetty_cfg.max_jfs = 256
        jetty_cfg.max_jfr = 256
        jetty_cfg.jfc_size = 4096
        
        jetty = urma_create_jetty(self._ctx, jetty_cfg)
        self._jfs = jetty.jfs
        self._jfc = jetty.jfc
        
        return True
    
    def register_memory(self, tensors: List, mem_type: str, 
                        dsva: bool = True) -> List[urma_seg_t]:
        segs = []
        
        for tensor in tensors:
            addr = tensor.untyped_storage().data_ptr()
            size = tensor.untyped_storage().nbytes()
            
            with self._lock:
                if addr in self._seg_cache:
                    self._seg_cache[addr].ref_count += 1
                    segs.append(self._seg_cache[addr])
                    continue
            
            seg_cfg = urma_seg_cfg_t()
            seg_cfg.va = addr
            seg_cfg.len = size
            seg_cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE
            seg_cfg.flag.bs.dsva = URMA_DSVA_ENABLE if dsva else URMA_DSVA_DISABLE
            
            seg = urma_register_seg(self._ctx, seg_cfg)
            seg.ref_count = 1
            
            with self._lock:
                self._seg_cache[addr] = seg
            
            segs.append(seg)
        
        return segs
    
    def get_transfer_descriptors(self, segs: List[urma_seg_t]) -> List[bytes]:
        return [self._serialize_seg(s) for s in segs]
    
    def read_from_remote(self, remote_agent_meta: bytes, 
                         local_segs: List[urma_seg_t],
                         remote_segs: List[urma_seg_t]) -> bool:
        remote_jetty = self._deserialize_jetty(remote_agent_meta)
        
        for local_seg, remote_seg in zip(local_segs, remote_segs):
            urma_read(
                self._jfs, remote_jetty,
                local_seg, remote_seg,
                local_seg.ubva.va, remote_seg.ubva.va,
                remote_seg.size, 0
            )
        
        while True:
            crs = urma_poll_jfc(self._jfc, 1)
            if len(crs) > 0:
                if crs[0].status == URMA_CR_SUCCESS:
                    return True
                else:
                    raise TransferError(crs[0].status)
            time.sleep(0.001)
    
    def deregister_memory(self, addr: int):
        with self._lock:
            if addr in self._seg_cache:
                seg = self._seg_cache[addr]
                seg.ref_count -= 1
                if seg.ref_count == 0:
                    urma_unregister_seg(seg)
                    self._seg_cache.pop(addr)
    
    def cleanup(self):
        with self._lock:
            for addr in list(self._seg_cache.keys()):
                urma_unregister_seg(self._seg_cache[addr])
                self._seg_cache.pop(addr)
        
        if self._ctx:
            urma_delete_context(self._ctx)
```

---

## 六、UBPU跨设备数据直通场景

### 6.1 场景一：NPU-GPU数据直通

```python
import ray
import torch
import torch_npu

ray.init(tensor_transport_backend="URMA")

@ray.remote(num_npus=1)
class NPUActor:
    def __init__(self):
        self.tensor = torch.randn(1024, 1024, device="npu:0")
    
    def get_tensor(self):
        return self.tensor  # 返回NPU张量

@ray.remote(num_gpus=1)
class GPUActor:
    def process_tensor(self, tensor):
        # tensor通过DSVA直通到GPU，无需CPU拷贝
        return tensor @ tensor.T  # GPU直接访问NPU数据

npu_actor = NPUActor.remote()
gpu_actor = GPUActor.remote()

tensor_ref = npu_actor.get_tensor.remote()
result = ray.get(gpu_actor.process_tensor.remote(tensor_ref))

# 传输过程：
# 1. NPU张量通过urma_register_seg注册，启用DSVA
# 2. 生成UBVA描述符（包含EID、UASID、VA）
# 3. GPU Actor通过urma_import_seg导入NPU内存
# 4. 使用urma_read直接从NPU读取数据到GPU
# 5. 无CPU参与，全程DMA传输

ray.shutdown()
```

### 6.2 场景二：跨节点NPU-NPU数据直通

```python
import ray
import torch
import torch_npu

# 节点A和节点B通过灵衢总线连接
ray.init(tensor_transport_backend="URMA")

@ray.remote(num_npus=1, placement_group=pg_node_a)
class NPUActorA:
    def __init__(self):
        self.tensor = torch.randn(10240, 10240, device="npu:0")  # 400MB
    
    def get_tensor(self):
        return self.tensor

@ray.remote(num_npus=1, placement_group=pg_node_b)
class NPUActorB:
    def receive_tensor(self, tensor):
        return tensor.sum()

actor_a = NPUActorA.remote()
actor_b = NPUActorB.remote()

tensor_ref = actor_a.get_tensor.remote()
result = ray.get(actor_b.receive_tensor.remote(tensor_ref))

# 传输过程：
# 1. NPU A注册内存，生成UBVA（包含节点EID）
# 2. NPU B通过EID发现节点A
# 3. urma_import_seg导入远端NPU内存
# 4. urma_read跨节点RDMA传输
# 5. 灵衢总线原生支持跨节点，无需额外IB配置

ray.shutdown()
```

### 6.3 场景三：CPU-NPU-GPU混合数据直通

```python
import ray
import torch
import torch_npu

ray.init(tensor_transport_backend="URMA")

@ray.remote(num_cpus=4)
class CPUActor:
    def generate_data(self):
        return torch.randn(1024, 1024)  # CPU张量

@ray.remote(num_npus=1)
class NPUActor:
    def process_on_npu(self, tensor):
        npu_tensor = tensor.to("npu:0")
        return npu_tensor @ npu_tensor.T

@ray.remote(num_gpus=1)
class GPUActor:
    def final_process(self, tensor):
        gpu_tensor = tensor.to("cuda:0")
        return gpu_tensor.sum()

cpu_actor = CPUActor.remote()
npu_actor = NPUActor.remote()
gpu_actor = GPUActor.remote()

# CPU -> NPU直通
cpu_data = cpu_actor.generate_data.remote()
npu_result = npu_actor.process_on_npu.remote(cpu_data)

# NPU -> GPU直通（DSVA跨设备）
final_result = ray.get(gpu_actor.final_process.remote(npu_result))

# UBPU数据流：
# UBPU-CPU(PUT) -> UBPU-NPU(DSVA) -> UBPU-GPU(DSVA)

ray.shutdown()
```

---

## 七、实施路线图

### 7.1 Phase 1：pyurma Python绑定开发（2周）

| 任务 | 时间 | 交付物 |
|-----|------|-------|
| pybind11绑定框架搭建 | 3天 | `_pyurma.so`基础框架 |
| 核心API绑定 | 5天 | `urma_init/register_seg/read/write`等 |
| 数据类型定义 | 2天 | `urma_seg_t/ubva_t/eid_t`等 |
| UrmaAgent高级封装 | 3天 | `UrmaAgent`类 |
| 单元测试 | 1天 | `test_pyurma.py` |

### 7.2 Phase 2：UBPU抽象层开发（2周）

| 任务 | 时间 | 交付物 |
|-----|------|-------|
| UBPUInfo类开发 | 2天 | `ubpu_info.py` |
| UBVAManager类开发 | 3天 | `ubva_manager.py` |
| TokenManager类开发 | 2天 | `token_manager.py` |
| 设备类型检测 | 2天 | `_detect_ubpu_type()` |
| UBPU间传输模式判定 | 3天 | `_determine_transfer_mode()` |
| 单元测试 | 2天 | `test_ubpu.py` |

### 7.3 Phase 3：URMA Tensor Transport开发（2周）

| 任务 | 时间 | 交付物 |
|-----|------|-------|
| UrmaTensorTransport类 | 5天 | `urma_tensor_transport.py` |
| UrmaTransportMetadata类 | 2天 | 元数据类 |
| NPU内存注册支持 | 2天 | DSVA配置 |
| URMA传输实现 | 3天 | `recv_multiple_tensors()` |
| 单元测试 | 2天 | `test_urma_transport.py` |

### 7.4 Phase 4：RAY集成与修改（1周）

| 任务 | 时间 | 交付物 |
|-----|------|-------|
| util.py修改 | 1天 | URMA注册逻辑 |
| rdt_manager.py修改 | 2天 | UBPU支持 |
| tensor_transport_manager.py修改 | 1天 | 新字段 |
| create_empty_tensors_from_metadata修改 | 1天 | NPU支持 |
| 集成测试 | 2天 | `test_rdt_urma.py` |

### 7.5 Phase 5：性能优化与生产化（2周）

| 任务 | 时间 | 交付物 |
|-----|------|-------|
| 批量传输优化 | 3天 | 多seg批量传输 |
| 内存注册缓存优化 | 2天 | 预注册机制 |
| 传输状态轮询优化 | 2天 | 事件驱动模式 |
| 错误处理完善 | 2天 | 异常处理 |
| 性能基准测试 | 3天 | 性能报告 |
| 文档编写 | 2天 | API文档 |

---

## 八、附录

### 8.1 关键头文件路径

| 模块 | 路径 |
|------|------|
| URMA API | `D:\C++\umdk-master\umdk-master\src\urma\lib\urma\core\include\urma_api.h` |
| URMA Types | `D:\C++\umdk-master\umdk-master\src\urma\lib\urma\core\include\urma_types.h` |
| URMA Opcode | `D:\C++\umdk-master\umdk-master\src\urma\lib\urma\core\include\urma_opcode.h` |
| URPC Message | `D:\C++\umdk-master\umdk-master\doc\ch\urpc\URPC Message.ch.md` |
| URMA API Guide | `D:\C++\umdk-master\umdk-master\doc\ch\urma\URMA API Guide.ch.md` |
| UDMA Driver | `D:\C++\umdk-master\umdk-master\src\urma\hw\udma\README-zh.md` |

### 8.2 RAY关键文件路径

| 模块 | 路径 |
|------|------|
| Tensor Transport Manager | `python/ray/experimental/rdt/tensor_transport_manager.py` |
| NIXL Transport | `python/ray/experimental/rdt/nixl_tensor_transport.py` |
| CUDA IPC Transport | `python/ray/experimental/rdt/cuda_ipc_transport.py` |
| Collective Transport | `python/ray/experimental/rdt/collective_tensor_transport.py` |
| RDT Manager | `python/ray/experimental/rdt/rdt_manager.py` |
| RDT Store | `python/ray/experimental/rdt/rdt_store.py` |
| Util Functions | `python/ray/experimental/rdt/util.py` |

### 8.3 API映射表

| RAY操作 | NIXL API | URMA API | 说明 |
|---------|---------|---------|------|
| 创建代理 | `nixl_agent()` | `urma_init()` + `urma_create_context()` | 初始化 |
| 创建端点 | `nixl_agent` | `urma_create_jetty()` | 创建Jetty |
| 注册内存 | `register_memory()` | `urma_register_seg()` | 内存注册 |
| 生成描述符 | `get_serialized_descs()` | `UBVADescriptor.to_bytes()` | 序列化 |
| 导入远端 | `add_remote_agent()` | `urma_import_seg()` + `urma_import_jetty()` | 导入 |
| 发起传输 | `transfer()` | `urma_read()` / `urma_write()` | DMA传输 |
| 检查状态 | `check_xfer_state()` | `urma_poll_jfc()` | 完成检查 |
| 释放资源 | `release_xfer_handle()` | 无需显式释放 | 自动管理 |
| 取消注册 | `deregister_memory()` | `urma_unregister_seg()` | 内存释放 |

---

*文档版本: 2.0*
*创建时间: 2025年*
*作者: AI分析助手*