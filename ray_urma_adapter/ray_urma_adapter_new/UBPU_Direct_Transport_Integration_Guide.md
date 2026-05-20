# RAY RDT 灵衢总线UBPU数据直通集成指南

## 一、概述

本指南说明如何将基于华为灵衢总线的UBPU数据直通能力集成到Ray RDT中，使Ray能够在灵衢总线超节点集群上实现CPU/NPU/GPU之间的数据相互直通。

## 二、核心概念

### 2.1 UBPU数据直通架构

```
┌───────────────────────────────────────────────────────────────┐
│                灵衢总线UBPU数据直通                             │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│   ┌──────────────────────────────────────────────────────┐   │
│   │                  UBPU统一抽象层                       │   │
│   ├─────────────┬─────────────┬─────────────┬────────────┤   │
│   │   UBPU-CPU  │  UBPU-NPU   │  UBPU-GPU   │  UBPU-DSP  │   │
│   │   (通用)    │  (昇腾)     │   (通用)    │   (专用)   │   │
│   └─────────────┴─────────────┴─────────────┴────────────┘   │
│                          │                                    │
│                          ▼                                    │
│   ┌──────────────────────────────────────────────────────┐   │
│   │                  UBVA统一地址空间                     │   │
│   │   (跨节点、跨设备统一编址)                            │   │
│   └──────────────────────────────────────────────────────┘   │
│                          │                                    │
│                          ▼                                    │
│   ┌──────────────────────────────────────────────────────┐   │
│   │                  DSVA数据直通                         │   │
│   │   (NPU/GPU设备内存直接访问)                           │   │
│   └──────────────────────────────────────────────────────┘   │
│                          │                                    │
│                          ▼                                    │
│   ┌──────────────────────────────────────────────────────┐   │
│   │                  URMA传输后端                         │   │
│   │   urma_read/write/send/recv/poll_jfc                 │   │
│   └──────────────────────────────────────────────────────┘   │
│                                                               │
│   关键优势：                                                  │
│   1. CPU/NPU/GPU统一抽象为UBPU                               │
│   2. UBVA打破节点边界，统一编址                              │
│   3. DSVA实现设备内存直通                                    │
│   4. 无CPU参与的DMA传输                                      │
│   5. 原生跨节点支持                                          │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### 2.2 传输模式

| 模式 | 源UBPU | 目标UBPU | 特点 | URMA操作 |
|-----|--------|---------|------|---------|
| IPC_LOCAL | UBPU-NPU | UBPU-NPU(同节点) | 同节点IPC | urma_import_seg |
| RDMA_DIRECT | UBPU-NPU | UBPU-NPU(跨节点) | 跨节点RDMA | urma_read |
| DSVA_CROSS_DEVICE | UBPU-NPU | UBPU-GPU | 跨设备直通 | urma_read(DSVA) |
| DSVA_ONE_SIDED | UBPU-NPU | UBPU-CPU | 单端DSVA | urma_read |
| RDMA_READ | UBPU-CPU | UBPU-CPU | CPU内存 | urma_read |

## 三、前置条件

### 3.1 系统要求

| 要求 | 说明 |
|-----|------|
| 操作系统 | EulerOS 22.03+ (推荐) 或 Linux |
| Ray版本 | Ray 2.55.1+ |
| UMDK版本 | UMDK 1.0+ |
| Python | Python 3.8+ |
| PyTorch | PyTorch 2.0+ |
| torch_npu | torch-npu 2.0+ (NPU支持) |
| 硬件 | 华为Ascend NPU (910B/310P) + 灵衢总线硬件 |

### 3.2 软件依赖

```bash
# Python依赖
pip install torch>=2.0.0
pip install torch-npu>=2.0.0  # Ascend PyTorch扩展
pip install pyurma>=1.0.0     # UMDK Python绑定

# 系统依赖
# UMDK库
liburma.so        # URMA库
liburpc.so        # URPC库
libcam.so         # CAM算子库

# 灵衢总线驱动
ubcore.ko         # 核心协议模块
uburma.ko         # URMA内核模块
udma.ko           # UDMA模块
ummu.ko           # 内存管理模块
```

### 3.3 驱动加载

```bash
# 加载灵衢总线驱动
cd /lib/modules/$(uname -r)/kernel/drivers
insmod ub/ubfi/ubfi.ko.xz cluster=1
insmod iommu/ummu-core/ummu-core.ko.xz
cd /lib/modules/$(uname -r)/kernel/drivers/ub/hisi-ub/kernelspace
insmod ummu/drivers/ummu.ko.xz ipver=609
insmod ubus/ubus.ko.xz ipver=609 cc_en=0
insmod ubase/ubase.ko.xz
insmod unic/unic.ko.xz
insmod cdma/cdma.ko.xz

# 加载URMA模块
modprobe ubcore
modprobe uburma
insmod ub/hisi-ub/kernelspace/udma/udma.ko.xz

# 添加权限
chmod -R 777 /usr/lib64/urma
chmod 777 /dev/ummu/tid
```

## 四、文件结构

### 4.1 Ray源码修改位置

```
ray-ray-2.55.1/
└── python/
    └── ray/
        └── experimental/
            └── rdt/
                ├── util.py                        # [修改] 注册URMA后端
                ├── tensor_transport_manager.py   # [修改] 添加UBPU支持
                ├── rdt_manager.py                # [修改] UBPU传输检测
                ├── ubpu_info.py                  # [新增] UBPU类型管理
                ├── ubva_manager.py               # [新增] UBVA地址管理
                ├── urma_tensor_transport.py      # [新增] URMA传输实现
                ├── ubpu_collective_transport.py  # [新增] UBPU集合通信
                ├── token_manager.py              # [新增] Token安全管理
                ├── npu_ipc_transport.py          # [新增] NPU IPC
                └── __init__.py                   # [修改] 导出新类
```

### 4.2 pyurma模块结构

```
pyurma/
├── __init__.py           # 模块入口，导出核心类
├── core.py               # 核心URMA API绑定
├── types.py              # 数据类型定义
├── agent.py              # UrmaAgent高级封装
├── ubva.py               # UBVA管理工具
├── ubpu.py               # UBPU管理工具
├── errors.py             # 错误定义
├── utils.py              # 工具函数
├── _pyurma.so            # C扩展模块(pybind11)
└── bindings/
    └── pyurma_bindings.cpp  # pybind11绑定代码
```

## 五、集成步骤

### 步骤1: 构建pyurma Python绑定

#### 1.1 创建pybind11绑定

```cpp
// pyurma/bindings/pyurma_bindings.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "urma_api.h"
#include "urma_types.h"

namespace py = pybind11;

PYBIND11_MODULE(_pyurma, m) {
    // URMA初始化
    m.def("urma_init", [](py::object config) {
        urma_init_attr_t *attr = nullptr;
        if (!config.is_none()) {
            attr = new urma_init_attr_t();
            attr->uasid = config["uasid"].cast<uint32_t>();
            attr->token = config["token"].cast<uint64_t>();
        }
        int ret = urma_init(attr);
        if (attr) delete attr;
        return ret;
    }, "Initialize URMA", py::arg("config") = py::none());
    
    m.def("urma_uninit", &urma_uninit, "Uninitialize URMA");
    
    // 设备管理
    m.def("urma_get_device_list", [](int *num) {
        auto devices = urma_get_device_list(num);
        py::list result;
        for (int i = 0; i < *num; i++) {
            result.append(py::capsule(devices[i], "urma_device"));
        }
        return result;
    });
    
    // Context管理
    m.def("urma_create_context", &urma_create_context);
    m.def("urma_delete_context", &urma_delete_context);
    
    // Segment管理
    py::class_<urma_seg_cfg_t>(m, "urma_seg_cfg_t")
        .def(py::init())
        .def_readwrite("va", &urma_seg_cfg_t::va)
        .def_readwrite("len", &urma_seg_cfg_t::len);
    
    py::class_<urma_seg_t>(m, "urma_seg_t")
        .def_property_readonly("ubva", [](urma_seg_t &s) {
            return py::make_tuple(s.ubva.eid.raw, s.ubva.uasid, s.ubva.va);
        });
    
    m.def("urma_register_seg", &urma_register_seg);
    m.def("urma_unregister_seg", &urma_unregister_seg);
    m.def("urma_import_seg", &urma_import_seg);
    m.def("urma_unimport_seg", &urma_unimport_seg);
    
    // Jetty管理
    m.def("urma_create_jetty", &urma_create_jetty);
    m.def("urma_create_jfs", &urma_create_jfs);
    m.def("urma_create_jfc", &urma_create_jfc);
    
    // 数据传输
    m.def("urma_read", [](urma_jfs_t *jfs, urma_jetty_id_t *jetty_id,
                          urma_seg_t *local_seg, urma_seg_t *remote_seg,
                          uint64_t dst_addr, uint64_t src_addr,
                          uint64_t length, uint64_t flags) {
        urma_sge_t dst_sge = {dst_addr, length, local_seg->token_id};
        urma_sge_t src_sge = {src_addr, length, remote_seg->token_id};
        urma_wr_t wr = {};
        wr.opcode = URMA_OPC_READ;
        wr.rw.dst.sge = &dst_sge;
        wr.rw.dst.num_sge = 1;
        wr.rw.src.sge = &src_sge;
        wr.rw.src.num_sge = 1;
        return urma_post_jfs_wr(jfs, jetty_id, &wr);
    });
    
    m.def("urma_write", [](urma_jfs_t *jfs, urma_jetty_id_t *jetty_id,
                           urma_seg_t *local_seg, urma_seg_t *remote_seg,
                           uint64_t dst_addr, uint64_t src_addr,
                           uint64_t length, uint64_t flags) {
        urma_sge_t dst_sge = {dst_addr, length, remote_seg->token_id};
        urma_sge_t src_sge = {src_addr, length, local_seg->token_id};
        urma_wr_t wr = {};
        wr.opcode = URMA_OPC_WRITE;
        wr.rw.dst.sge = &dst_sge;
        wr.rw.dst.num_sge = 1;
        wr.rw.src.sge = &src_sge;
        wr.rw.src.num_sge = 1;
        return urma_post_jfs_wr(jfs, jetty_id, &wr);
    });
    
    // 完成队列
    m.def("urma_poll_jfc", [](urma_jfc_t *jfc, int cr_cnt) {
        urma_cr_t *crs = new urma_cr_t[cr_cnt];
        int count = urma_poll_jfc(jfc, cr_cnt, crs);
        py::list result;
        for (int i = 0; i < count; i++) {
            result.append(py::make_tuple(crs[i].status, crs[i].length));
        }
        delete[] crs;
        return result;
    });
    
    m.def("urma_wait_jfc", &urma_wait_jfc);
    
    // 常量定义
    m.attr("URMA_ACCESS_READ") = URMA_ACCESS_READ;
    m.attr("URMA_ACCESS_WRITE") = URMA_ACCESS_WRITE;
    m.attr("URMA_ACCESS_ATOMIC") = URMA_ACCESS_ATOMIC;
    m.attr("URMA_DSVA_ENABLE") = URMA_DSVA_ENABLE;
    m.attr("URMA_DSVA_DISABLE") = URMA_DSVA_DISABLE;
    m.attr("URMA_CR_SUCCESS") = URMA_CR_SUCCESS;
}
```

#### 1.2 编译

```bash
# 创建setup.py
cd pyurma
cat > setup.py << 'EOF'
from setuptools import setup, Extension
from pybind11.setup_helpers import Pybind11Extension

ext_modules = [
    Pybind11Extension(
        "_pyurma",
        ["bindings/pyurma_bindings.cpp"],
        include_dirs=[
            "/usr/include/ub/umdk/urma/core",
            "/usr/include/ub/umdk/urma",
        ],
        library_dirs=["/usr/lib64/urma"],
        libraries=["urma"],
        extra_compile_args=["-std=c++17"],
    )
]

setup(
    name="pyurma",
    version="1.0.0",
    ext_modules=ext_modules,
    packages=["pyurma"],
)
EOF

# 编译安装
python setup.py build_ext --inplace
python setup.py install
```

#### 1.3 验证

```python
# test_pyurma.py
from pyurma import urma_init, urma_get_device_list

urma_init(None)
devices = urma_get_device_list()
print(f"URMA initialized, found {len(devices)} devices")
```

### 步骤2: 创建UBPU抽象层

#### 2.1 ubpu_info.py

```python
# python/ray/experimental/rdt/ubpu_info.py
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional
import struct

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
    NPU_ASCEND_910C = 0x003
    GPU_GENERAL = 0x001
    GPU_NVIDIA_A100 = 0x002

@dataclass
class UBPUInfo:
    ubpu_class: UBPUClass
    ubpu_subclass: UBPUSubclass
    device_id: int
    node_eid: bytes  # 16 bytes
    uasid: int
    
    def to_bytes(self) -> bytes:
        return struct.pack(
            '<HHI16sI',
            self.ubpu_class,
            self.ubpu_subclass,
            self.device_id,
            self.node_eid,
            self.uasid
        )
    
    @staticmethod
    def from_bytes(data: bytes) -> 'UBPUInfo':
        ubpu_class, ubpu_subclass, device_id, node_eid, uasid = struct.unpack(
            '<HHI16sI', data
        )
        return UBPUInfo(
            UBPUClass(ubpu_class),
            UBPUSubclass(ubpu_subclass),
            device_id,
            node_eid,
            uasid
        )
    
    def to_function_field(self, method: int, private: bool = False) -> int:
        p_flag = 1 if private else 0
        return (self.ubpu_class << 36) | (self.ubpu_subclass << 24) | (p_flag << 23) | method
    
    def is_dsva_capable(self) -> bool:
        return self.ubpu_class in [UBPUClass.NPU, UBPUClass.GPU]
    
    def is_same_node(self, other: 'UBPUInfo') -> bool:
        return self.node_eid == other.node_eid
    
    def is_same_class(self, other: 'UBPUInfo') -> bool:
        return self.ubpu_class == other.ubpu_class
    
    @staticmethod
    def from_torch_device(device, urma_context=None) -> 'UBPUInfo':
        import ray
        
        ctx = ray.get_runtime_context()
        node_id = ctx.get_node_id()
        
        node_eid = _get_node_eid(node_id)
        uasid = urma_context.uasid if urma_context else 0
        
        if device.type == "npu":
            return UBPUInfo(
                ubpu_class=UBPUClass.NPU,
                ubpu_subclass=_detect_npu_type(device),
                device_id=device.index if device.index else 0,
                node_eid=node_eid,
                uasid=uasid
            )
        elif device.type == "cuda":
            return UBPUInfo(
                ubpu_class=UBPUClass.GPU,
                ubpu_subclass=UBPUSubclass.GPU_GENERAL,
                device_id=device.index if device.index else 0,
                node_eid=node_eid,
                uasid=uasid
            )
        else:
            return UBPUInfo(
                ubpu_class=UBPUClass.CPU,
                ubpu_subclass=UBPUSubclass.CPU_GENERAL,
                device_id=-1,
                node_eid=node_eid,
                uasid=uasid
            )

def _detect_npu_type(device) -> UBPUSubclass:
    import torch_npu
    name = torch.npu.get_device_name(device.index)
    if "910B" in name:
        return UBPUSubclass.NPU_ASCEND_910B
    elif "310P" in name:
        return UBPUSubclass.NPU_ASCEND_310P
    elif "910C" in name:
        return UBPUSubclass.NPU_ASCEND_910C
    return UBPUSubclass.NPU_ASCEND_910B

def _get_node_eid(node_id: str) -> bytes:
    from pyurma import urma_get_device_list
    devices = urma_get_device_list()
    if len(devices) > 0:
        return devices[0].eid.raw
    return bytes(16)
```

#### 2.2 ubva_manager.py

```python
# python/ray/experimental/rdt/ubva_manager.py
from dataclasses import dataclass
from typing import Dict, Optional
import threading
import struct

@dataclass
class UBVADescriptor:
    eid: bytes       # 16 bytes
    uasid: int       # 4 bytes
    va: int          # 8 bytes
    size: int        # 8 bytes
    dsva_enabled: bool  # 1 byte
    token_id: int    # 4 bytes
    
    def to_bytes(self) -> bytes:
        dsva_flag = 1 if self.dsva_enabled else 0
        return struct.pack(
            '<16sIQBI',
            self.eid,
            self.uasid,
            self.va,
            self.size,
            dsva_flag,
            self.token_id
        )
    
    @staticmethod
    def from_bytes(data: bytes) -> 'UBVADescriptor':
        eid, uasid, va, size, dsva_flag, token_id = struct.unpack(
            '<16sIQBI', data
        )
        return UBVADescriptor(
            eid=eid,
            uasid=uasid,
            va=va,
            size=size,
            dsva_enabled=bool(dsva_flag),
            token_id=token_id
        )
    
    def get_ubva(self) -> tuple:
        return (self.eid, self.uasid, self.va)

class UBVAManager:
    def __init__(self, urma_context, urma_jetty):
        self._context = urma_context
        self._jetty = urma_jetty
        self._seg_cache: Dict[int, tuple] = {}  # addr -> (seg, descriptor)
        self._remote_seg_cache: Dict[bytes, any] = {}  # descriptor bytes -> seg
        self._lock = threading.RLock()
    
    def register_memory(self, addr: int, size: int, ubpu_info, dsva: bool = True):
        from pyurma import (
            urma_register_seg, urma_seg_cfg_t,
            URMA_ACCESS_READ, URMA_ACCESS_WRITE, URMA_DSVA_ENABLE
        )
        
        with self._lock:
            if addr in self._seg_cache:
                seg, desc = self._seg_cache[addr]
                return desc
            
            seg_cfg = urma_seg_cfg_t()
            seg_cfg.va = addr
            seg_cfg.len = size
            seg_cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE
            seg_cfg.flag.bs.dsva = URMA_DSVA_ENABLE if dsva else URMA_DSVA_DISABLE
            
            seg = urma_register_seg(self._context, seg_cfg)
            
            descriptor = UBVADescriptor(
                eid=seg.ubva.eid.raw,
                uasid=seg.ubva.uasid,
                va=seg.ubva.va,
                size=size,
                dsva_enabled=dsva,
                token_id=seg.token_id
            )
            
            self._seg_cache[addr] = (seg, descriptor)
            
            return descriptor
    
    def import_remote_memory(self, descriptor: UBVADescriptor):
        from pyurma import urma_import_seg, urma_seg_cfg_t
        
        descriptor_bytes = descriptor.to_bytes()
        
        with self._lock:
            if descriptor_bytes in self._remote_seg_cache:
                return self._remote_seg_cache[descriptor_bytes]
            
            seg_cfg = urma_seg_cfg_t()
            seg_cfg.ubva.eid.raw = descriptor.eid
            seg_cfg.ubva.uasid = descriptor.uasid
            seg_cfg.ubva.va = descriptor.va
            
            remote_seg = urma_import_seg(self._context, seg_cfg)
            self._remote_seg_cache[descriptor_bytes] = remote_seg
            
            return remote_seg
    
    def unregister_memory(self, addr: int):
        from pyurma import urma_unregister_seg
        
        with self._lock:
            if addr in self._seg_cache:
                seg, _ = self._seg_cache[addr]
                urma_unregister_seg(seg)
                self._seg_cache.pop(addr)
    
    def unimport_remote_memory(self, descriptor: UBVADescriptor):
        from pyurma import urma_unimport_seg
        
        descriptor_bytes = descriptor.to_bytes()
        
        with self._lock:
            if descriptor_bytes in self._remote_seg_cache:
                urma_unimport_seg(self._remote_seg_cache[descriptor_bytes])
                self._remote_seg_cache.pop(descriptor_bytes)
```

### 步骤3: 创建URMA Tensor Transport

#### 3.1 urma_tensor_transport.py

```python
# python/ray/experimental/rdt/urma_tensor_transport.py
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray._private.ray_constants import NIXL_REMOTE_AGENT_CACHE_MAXSIZE
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
    urma_agent_version: Optional[int] = 0
    dsva_enabled: bool = True

class UrmaTensorTransport(TensorTransportManager):
    def __init__(self):
        self._urma_ctx = None
        self._urma_jetty = None
        self._urma_jfs = None
        self._urma_jfc = None
        self._ubva_manager: Optional[UBVAManager] = None
        self._remote_jettys: OrderedDict = OrderedDict()
        self._aborted_transfer_obj_ids = set()
        self._aborted_lock = threading.Lock()
        self._cache_lock = threading.RLock()
        self._version = 0
    
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
            
            self._ubva_manager = UBVAManager(self._urma_ctx, self._urma_jetty)
        
        return self._urma_ctx
    
    def actor_has_tensor_transport(self, actor) -> bool:
        def __check__(self):
            try:
                from ray.experimental.rdt.util import get_tensor_transport_manager
                mgr = get_tensor_transport_manager("URMA")
                mgr.get_urma_context()
                return True
            except:
                return False
        
        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(__check__)
        )
    
    def extract_tensor_transport_metadata(self, obj_id: str, rdt_object: List):
        import torch
        
        with self._cache_lock:
            device = None
            tensor_meta = []
            ubva_descriptors = []
            
            if rdt_object:
                device = rdt_object[0].device
                urma_ctx = self.get_urma_context()
                ubpu_info = UBPUInfo.from_torch_device(device, urma_ctx)
                
                for t in rdt_object:
                    if t.device.type != device.type:
                        raise ValueError("All tensors must have same device type")
                    if not t.is_contiguous():
                        raise ValueError("All tensors must be contiguous")
                    tensor_meta.append((t.shape, t.dtype))
                
                if device.type in ["npu", "cuda"]:
                    devices = set(t.device for t in rdt_object)
                    for dev in devices:
                        if device.type == "npu":
                            torch_npu.npu.synchronize(dev)
                        else:
                            torch.cuda.synchronize(dev)
                
                for tensor in rdt_object:
                    desc = self._ubva_manager.register_memory(
                        addr=tensor.untyped_storage().data_ptr(),
                        size=tensor.untyped_storage().nbytes(),
                        ubpu_info=ubpu_info,
                        dsva=ubpu_info.is_dsva_capable()
                    )
                    ubva_descriptors.append(desc.to_bytes())
                
                return UrmaTransportMetadata(
                    tensor_meta=tensor_meta,
                    tensor_device=device.type,
                    ubva_descriptors=ubva_descriptors,
                    src_ubpu_info=ubpu_info.to_bytes(),
                    urma_agent_eid=urma_ctx.eid.raw,
                    urma_agent_uasid=urma_ctx.uasid,
                    urma_agent_version=self._version,
                    dsva_enabled=ubpu_info.is_dsva_capable()
                )
            
            return UrmaTransportMetadata(
                tensor_meta=tensor_meta,
                tensor_device=None,
                ubva_descriptors=None,
                src_ubpu_info=None,
                urma_agent_eid=None,
                urma_agent_uasid=None,
                urma_agent_version=0,
                dsva_enabled=False
            )
    
    def get_communicator_metadata(self, src_actor, dst_actor, backend=None):
        src_ubpu = self._get_actor_ubpu(src_actor)
        dst_ubpu = self._get_actor_ubpu(dst_actor)
        
        transfer_mode = self._determine_transfer_mode(src_ubpu, dst_ubpu)
        
        return UrmaCommunicatorMetadata(
            src_ubpu=src_ubpu,
            dst_ubpu=dst_ubpu,
            transfer_mode=transfer_mode
        )
    
    def _determine_transfer_mode(self, src_ubpu: UBPUInfo, dst_ubpu: UBPUInfo) -> str:
        if src_ubpu.is_same_node(dst_ubpu):
            if src_ubpu.is_same_class(dst_ubpu):
                return "IPC_LOCAL"
            else:
                if src_ubpu.is_dsva_capable() and dst_ubpu.is_dsva_capable():
                    return "DSVA_CROSS_DEVICE"
                else:
                    return "DSVA_ONE_SIDED"
        else:
            if src_ubpu.is_dsva_capable():
                return "RDMA_DIRECT"
            else:
                return "RDMA_READ"
    
    def _get_actor_ubpu(self, actor) -> UBPUInfo:
        def __get_ubpu__(self):
            import torch
            import torch_npu
            from ray.experimental.rdt.util import get_tensor_transport_manager
            
            mgr = get_tensor_transport_manager("URMA")
            ctx = mgr.get_urma_context()
            
            if torch.npu.is_available():
                device = torch.device("npu:0")
                return UBPUInfo.from_torch_device(device, ctx)
            elif torch.cuda.is_available():
                device = torch.device("cuda:0")
                return UBPUInfo.from_torch_device(device, ctx)
            else:
                return UBPUInfo.from_torch_device(torch.device("cpu"), ctx)
        
        ubpu_bytes = ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(__get_ubpu__)
        )
        return UBPUInfo.from_bytes(ubpu_bytes)
    
    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List] = None,
    ):
        from ray.experimental.rdt.util import create_empty_tensors_from_metadata
        from pyurma import urma_import_jetty, urma_poll_jfc, URMA_CR_SUCCESS
        
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
        
        dst_ubpu = UBPUInfo.from_torch_device(tensors[0].device, urma_ctx)
        
        local_descriptors = []
        for tensor in tensors:
            desc = self._ubva_manager.register_memory(
                addr=tensor.untyped_storage().data_ptr(),
                size=tensor.untyped_storage().nbytes(),
                ubpu_info=dst_ubpu,
                dsva=dst_ubpu.is_dsva_capable()
            )
            local_descriptors.append(desc)
        
        try:
            remote_jetty_eid = tensor_transport_metadata.urma_agent_eid
            remote_jetty_uasid = tensor_transport_metadata.urma_agent_uasid
            
            remote_jetty_id = urma_import_jetty(urma_ctx, remote_jetty_eid, remote_jetty_uasid)
            
            for local_desc, remote_desc in zip(local_descriptors, remote_descriptors):
                remote_seg = self._ubva_manager.import_remote_memory(remote_desc)
                
                from pyurma import urma_read
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
                crs = urma_poll_jfc(self._urma_jfc, len(local_descriptors))
                all_success = True
                for cr in crs:
                    if cr[0] != URMA_CR_SUCCESS:
                        raise RuntimeError(f"URMA transfer failed: {cr[0]}")
                    if cr[0] == URMA_CR_SUCCESS and cr[1] == remote_desc.size:
                        all_success = True
                
                if all_success and len(crs) == len(local_descriptors):
                    break
                
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
    
    def send_multiple_tensors(self, tensors, metadata, comm_meta):
        raise NotImplementedError("URMA is one-sided transport")
    
    def garbage_collect(self, obj_id, tensor_transport_meta, tensors):
        with self._cache_lock:
            for tensor in tensors:
                addr = tensor.untyped_storage().data_ptr()
                self._ubva_manager.unregister_memory(addr)
            self._version += 1
    
    def abort_transport(self, obj_id, communicator_metadata):
        with self._aborted_lock:
            self._aborted_transfer_obj_ids.add(obj_id)
```

### 步骤4: 修改Ray RDT源码

#### 4.1 修改util.py

```python
# python/ray/experimental/rdt/util.py
# 添加以下内容

# 修改DEFAULT_TRANSPORTS
DEFAULT_TRANSPORTS = ["URMA", "NIXL", "GLOO", "NCCL", "CUDA_IPC"]

# 添加检测函数
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

# 修改_ensure_default_transports_registered
def _ensure_default_transports_registered():
    global _default_transports_registered
    with transport_managers_lock:
        if _default_transports_registered:
            return
        _default_transports_registered = True
        try:
            import torch
            
            # 注册URMA后端（优先）
            if _check_urma_available():
                from ray.experimental.rdt.urma_tensor_transport import UrmaTensorTransport
                register_tensor_transport(
                    "URMA", ["npu", "cuda", "cpu"], UrmaTensorTransport, torch.Tensor
                )
            
            # 注册NPU IPC后端
            if _check_torch_npu_available():
                from ray.experimental.rdt.npu_ipc_transport import NpuIpcTransport
                register_tensor_transport(
                    "NPU_IPC", ["npu"], NpuIpcTransport, torch.Tensor
                )
            
            # 保持原有后端
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
        except ImportError:
            pass

# 修改create_empty_tensors_from_metadata
def create_empty_tensors_from_metadata(
    tensor_transport_meta: TensorTransportMetadata,
) -> List["torch.Tensor"]:
    import torch
    
    tensors = []
    device = tensor_transport_meta.tensor_device
    
    for meta in tensor_transport_meta.tensor_meta:
        shape, dtype = meta
        
        # 支持NPU设备
        if device == "npu":
            try:
                import torch_npu
                tensor = torch.empty(shape, dtype=dtype, device=f"npu:{torch.npu.current_device()}")
            except ImportError:
                tensor = torch.empty(shape, dtype=dtype, device="cpu")
        elif device == "cuda":
            tensor = torch.empty(shape, dtype=dtype, device=device)
        else:
            tensor = torch.empty(shape, dtype=dtype, device="cpu")
        
        tensors.append(tensor)
    
    return tensors
```

#### 4.2 修改rdt_manager.py

```python
# python/ray/experimental/rdt/rdt_manager.py
# 添加以下内容

# 修改RDTMeta
class RDTMeta(NamedTuple):
    src_actor: "ray.actor.ActorHandle"
    tensor_transport_backend: str
    tensor_transport_meta: Optional["TensorTransportMetadata"]
    sent_dest_actors: Set[str]
    sent_to_src_actor_and_others_warned: bool
    target_buffers: Optional[List[weakref.ReferenceType[Any]]]
    src_ubpu_info: Optional[bytes]  # 新增
    dst_ubpu_info: Optional[bytes]  # 新增

# 修改trigger_out_of_band_tensor_transfer
def trigger_out_of_band_tensor_transfer(self, dst_actor, obj_id):
    from ray.experimental.rdt.ubpu_info import UBPUInfo
    
    rdt_meta = self._managed_rdt_metadata[obj_id]
    src_actor = rdt_meta.src_actor
    tensor_transport = rdt_meta.tensor_transport_backend
    
    src_ubpu = None
    dst_ubpu = None
    
    if rdt_meta.src_ubpu_info:
        src_ubpu = UBPUInfo.from_bytes(rdt_meta.src_ubpu_info)
    
    if src_ubpu and tensor_transport == "URMA":
        dst_ubpu = self._get_actor_ubpu(dst_actor)
        
        transfer_mode = self._determine_ubpu_transfer_mode(src_ubpu, dst_ubpu)
        
        if transfer_mode == "IPC_LOCAL":
            tensor_transport = "URMA_IPC"
        elif transfer_mode == "DSVA_CROSS_DEVICE":
            tensor_transport = "URMA_DSVA"
        elif transfer_mode == "RDMA_DIRECT":
            tensor_transport = "URMA_RDMA"
    
    tensor_transport_manager = get_tensor_transport_manager(tensor_transport)
    communicator_meta = tensor_transport_manager.get_communicator_metadata(
        src_actor, dst_actor, tensor_transport
    )
    
    # ... 其余逻辑保持不变

def _get_actor_ubpu(self, actor) -> UBPUInfo:
    def __get_ubpu__(self):
        from ray.experimental.rdt.util import get_tensor_transport_manager
        mgr = get_tensor_transport_manager("URMA")
        ctx = mgr.get_urma_context()
        import torch
        device = torch.device("npu:0") if torch.npu.is_available() else torch.device("cuda:0")
        return UBPUInfo.from_torch_device(device, ctx).to_bytes()
    
    return UBPUInfo.from_bytes(
        ray.get(actor.__ray_call__.options(concurrency_group="_ray_system").remote(__get_ubpu__))
    )

def _determine_ubpu_transfer_mode(self, src: UBPUInfo, dst: UBPUInfo) -> str:
    if src.is_same_node(dst):
        if src.is_same_class(dst):
            return "IPC_LOCAL"
        else:
            return "DSVA_CROSS_DEVICE"
    else:
        return "RDMA_DIRECT"
```

## 六、使用示例

### 6.1 NPU数据传输

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
        return self.tensor
    
    def process(self, tensor):
        return tensor @ tensor.T

actor1 = NPUActor.remote()
actor2 = NPUActor.remote()

# NPU间DSVA直通传输
tensor_ref = actor1.get_tensor.remote()
result = ray.get(actor2.process.remote(tensor_ref))

print(f"Result shape: {result.shape}, device: {result.device}")

ray.shutdown()
```

### 6.2 跨节点NPU传输

```python
import ray
import torch
import torch_npu

# 节点A和节点B通过灵衢总线连接
ray.init()

@ray.remote(num_npus=1, resources={"node_a": 1})
class NPUActorA:
    def __init__(self):
        self.tensor = torch.randn(10240, 10240, device="npu:0")
    
    def get_tensor(self):
        return self.tensor

@ray.remote(num_npus=1, resources={"node_b": 1})
class NPUActorB:
    def process(self, tensor):
        return tensor.sum().item()

actor_a = NPUActorA.remote()
actor_b = NPUActorB.remote()

# 跨节点UBVA直通
tensor_ref = actor_a.get_tensor.remote()
result = ray.get(actor_b.process.remote(tensor_ref))

print(f"Sum result: {result}")

ray.shutdown()
```

### 6.3 NPU-GPU跨设备传输

```python
import ray
import torch
import torch_npu

ray.init()

@ray.remote(num_npus=1)
class NPUActor:
    def create_tensor(self):
        return torch.randn(1024, 1024, device="npu:0")

@ray.remote(num_gpus=1)
class GPUActor:
    def process_npu_tensor(self, npu_tensor):
        # DSVA跨设备直通：NPU数据直接访问
        gpu_tensor = npu_tensor.to("cuda:0")
        return gpu_tensor @ gpu_tensor.T

npu_actor = NPUActor.remote()
gpu_actor = GPUActor.remote()

# NPU -> GPU DSVA直通
npu_tensor_ref = npu_actor.create_tensor.remote()
result = ray.get(gpu_actor.process_npu_tensor.remote(npu_tensor_ref))

print(f"Cross-device result: {result.shape}")

ray.shutdown()
```

## 七、性能优化

### 7.1 内存预注册

```python
from ray.experimental.rdt.util import register_urma_memory

@ray.remote(num_npus=1)
class NPUActor:
    def __init__(self):
        self.weight = torch.randn(1000, 1000, device="npu:0")
        # 预注册内存，避免每次传输时注册
        register_urma_memory(self.weight)
    
    def get_weight(self):
        return self.weight  # 直接使用预注册的内存
```

### 7.2 批量传输

```python
@ray.remote(num_npus=1)
class NPUBatchActor:
    def get_tensors(self):
        tensors = [torch.randn(100, 100, device="npu:0") for _ in range(10)]
        return tensors  # 一次性传输多个张量
    
    def process_batch(self, tensors):
        results = [t @ t.T for t in tensors]
        return results
```

### 7.3 传输路径优化

```python
# 预建立传输路径
from pyurma import urma_advise_jetty, URMA_ADVICE_CREATE_TP

urma_advise_jetty(
    ctx, local_jetty, remote_jetty,
    URMA_ADVICE_CREATE_TP,
    timeout=5000
)
```

## 八、调试与问题排查

### 8.1 UBPU诊断

```python
# diagnose_ubpu.py
import torch
import torch_npu
from ray.experimental.rdt.ubpu_info import UBPUInfo

def diagnose():
    print("=== UBPU Diagnostics ===")
    
    print(f"NPU available: {torch.npu.is_available()}")
    print(f"NPU count: {torch.npu.device_count()}")
    
    for i in range(torch.npu.device_count()):
        name = torch.npu.get_device_name(i)
        print(f"NPU {i}: {name}")
        
        device = torch.device(f"npu:{i}")
        ubpu = UBPUInfo.from_torch_device(device)
        print(f"  UBPU Class: {ubpu.ubpu_class.name}")
        print(f"  UBPU Subclass: {ubpu.ubpu_subclass.name}")
        print(f"  DSVA capable: {ubpu.is_dsva_capable()}")
    
    print("=== Diagnostics Complete ===")

diagnose()
```

### 8.2 常见问题

| 问题 | 原因 | 解决方案 |
|-----|------|---------|
| URMA初始化失败 | liburma.so未找到 | 安装UMDK并设置LD_LIBRARY_PATH |
| DSVA启用失败 | NPU驱动不支持 | 更新NPU驱动版本 |
| 跨节点传输失败 | EID配置错误 | 检查灵衢总线配置 |
| Token验证失败 | Token策略不匹配 | 确认两端使用相同Token策略 |
| NPU内存注册失败 | torch_npu未安装 | 安装torch-npu |
| UBVA导入失败 | UASID不匹配 | 检查上下文配置 |

## 九、部署清单

### 9.1 生产环境部署

```bash
# 1. 安装UMDK
yum install umdk urma urpc cam

# 2. 安装torch_npu
pip install torch-npu==2.0.0

# 3. 安装pyurma
cd pyurma
python setup.py install

# 4. 安装修改后的Ray
cd ray-ray-2.55.1
python setup.py install

# 5. 配置环境变量
export LD_LIBRARY_PATH=/usr/lib64/urma:$LD_LIBRARY_PATH
export URMA_LOG_LEVEL=info
export RAY_TENSOR_TRANSPORT_BACKEND=URMA

# 6. 加载驱动
modprobe ubcore
modprobe uburma
insmod /lib/modules/$(uname -r)/kernel/drivers/ub/hisi-ub/kernelspace/udma/udma.ko.xz
```

---

*文档版本: 2.0*
*创建时间: 2025年*