# Ray架构适配灵衢总线技术方案

## 一、架构分析

### 1.1 Ray现有GPU通信架构层次

```
应用层：用户代码（ray.remote, compiled DAG）
    ↓
抽象层：Backend接口 + BaseGroup抽象类
    ↓
实现层：NCCLGroup / GLOOGroup / CUDA_IPC Transport
    ↓
硬件层：cupy.cuda.nccl → NVIDIA NCCL库
    ↓
物理层：NVLink / PCIe / Network
```

### 1.2 灵衢适配改造层次图

```
应用层：用户代码（无需改动）
    ↓
抽象层：Backend增加LINGQU + BaseGroup扩展
    ↓
实现层：新增 LingquGroup + LingquTransport
    ↓
硬件层：lingqu_sdk（灵衢SDK Python绑定）
    ↓
物理层：灵衢总线（CPU/GPU互联）
```

---

## 二、需要修改的关键模块

### 2.1 Python层核心修改（共7个模块）

#### 模块1：Backend类型扩展
**文件**：`python/ray/util/collective/types.py`

**修改内容**：
```python
# 原代码（Line 34-39）
class Backend(object):
    NCCL = "NCCL"
    GLOO = "GLOO"
    UNRECOGNIZED = "unrecognized"

# 新增代码
class Backend(object):
    NCCL = "NCCL"
    GLOO = "GLOO"
    LINGQU = "LINGQU"         # 新增
    LINGQU_SHM = "LINGQU_SHM" # 灵衢内存共享
    UNRECOGNIZED = "unrecognized"
    
    def __new__(cls, name: str):
        upper_name = name.upper()
        backend = getattr(Backend, upper_name, Backend.UNRECOGNIZED)
        if backend == Backend.UNRECOGNIZED:
            if upper_name == "TORCH_GLOO":
                return Backend.GLOO
            # 新增灵衢别名支持
            if upper_name in ["LINGQU_BUS", "LQ"]:
                return Backend.LINGQU
            raise ValueError(
                f"Unrecognized backend: '{name}'. "
                f"Supported: NCCL, GLOO, LINGQU"
            )
        return backend
```

---

#### 模块2：BaseGroup抽象接口保持不变
**文件**：`python/ray/util/collective/collective_group/base_collective_group.py`

**无需修改**，但需确保灵衢实现所有抽象方法：
```python
class BaseGroup(metaclass=ABCMeta):
    @abstractmethod
    def allreduce(self, tensor, allreduce_options)
    @abstractmethod
    def allgather(self, tensor_list, tensor, allgather_options)
    @abstractmethod
    def broadcast(self, tensor, broadcast_options)
    @abstractmethod
    def send(self, tensor, send_options)
    @abstractmethod
    def recv(self, tensor, recv_options)
    # ... 其他方法
```

---

#### 模块3：新增灵衢通信组实现（核心）
**文件**：`python/ray/util/collective/collective_group/lingqu_collective_group.py`（新建）

**实现要点**：

```python
import logging
from typing import List, Optional

# 导入灵衢SDK（假设提供的Python绑定）
try:
    import lingqu_sdk
    from lingqu_sdk import (
        LingquCommunicator,
        LingquUniqueId,
        LingquMemoryHandle,
        lingqu_group_start,
        lingqu_group_end
    )
    _LINGQU_AVAILABLE = True
except ImportError:
    _LINGQU_AVAILABLE = False

from ray.util.collective.collective_group.base_collective_group import BaseGroup
from ray.util.collective.types import (
    AllReduceOptions, AllGatherOptions, BroadcastOptions,
    SendOptions, RecvOptions, ReduceOp
)

logger = logging.getLogger(__name__)


class LingquGroup(BaseGroup):
    """灵衢总线通信组实现"""
    
    def __init__(self, world_size: int, rank: int, group_name: str):
        super().__init__(world_size, rank, group_name)
        
        # 通信器缓存（类似NCCL）
        self._dev_comm_map = {}      # LingquCommunicator缓存
        self._memory_handle_map = {} # 灵衢内存句柄缓存
        self._used_device_indices = set()
        
        # 灵衢版本检查
        if not _LINGQU_AVAILABLE:
            raise ImportError(
                "灵衢SDK未安装。请安装: pip install lingqu-sdk"
            )
        
        # 检查灵衢SDK版本
        sdk_version = lingqu_sdk.get_version()
        if sdk_version < "1.0.0":
            raise RuntimeError(f"灵衢SDK版本过低: {sdk_version}，需要 >= 1.0.0")
    
    @classmethod
    def backend(cls):
        return "LINGQU"
    
    def _get_lingqu_communicator(self, comm_key: str, devices: List[int]):
        """获取或创建灵衢通信器
        
        关键实现：
        1. 使用灵衢总线建立通信组
        2. 支持CPU-GPU、GPU-GPU混合通信
        3. 利用灵衢内存共享机制
        """
        if comm_key in self._dev_comm_map:
            return self._dev_comm_map[comm_key]
        
        # Rank 0生成灵衢UniqueId
        if self.rank == 0:
            lingqu_uid = lingqu_sdk.generate_unique_id()
            # 存储到Ray Named Actor（类似NCCL）
            self._store_unique_id(comm_key, lingqu_uid)
        else:
            # 其他rank从Ray Named Actor获取UID
            lingqu_uid = self._retrieve_unique_id(comm_key)
        
        # 创建灵衢通信器
        lingqu_sdk.group_start()
        comms = []
        for device_idx in devices:
            actual_rank = self.rank * len(devices) + device_idx
            comm = LingquCommunicator(
                world_size=self.world_size * len(devices),
                unique_id=lingqu_uid,
                rank=actual_rank,
                bus_type="LINGQU_BUS"  # 指定灵衢总线
            )
            comms.append(comm)
            self._used_device_indices.add(device_idx)
        lingqu_sdk.group_end()
        
        self._dev_comm_map[comm_key] = comms
        return comms
    
    def allreduce(self, tensors: List, options: AllReduceOptions):
        """灵衢AllReduce实现
        
        利用灵衢总线特性：
        1. 自动选择最优路径（CPU-CPU/GPU-GPU/GPU-CPU）
        2. 支持零拷贝内存共享
        """
        devices = self._get_tensor_devices(tensors)
        comm_key = self._generate_comm_key(devices)
        comms = self._get_lingqu_communicator(comm_key, devices)
        
        # 映射ReduceOp
        lingqu_op = self._map_reduce_op(options.reduceOp)
        
        lingqu_sdk.group_start()
        for i, tensor in enumerate(tensors):
            # 灵衢特有：自动检测tensor位置并选择最优路径
            tensor_location = self._detect_tensor_location(tensor)
            
            if tensor_location == "GPU":
                # GPU-to-GPU通过灵衢总线
                comms[i].all_reduce_gpu(
                    self._get_tensor_ptr(tensor),
                    self._get_tensor_n_elements(tensor),
                    self._get_tensor_dtype(tensor),
                    lingqu_op
                )
            else:
                # CPU-to-CPU或CPU-to-GPU混合
                comms[i].all_reduce_hybrid(
                    self._get_tensor_ptr(tensor),
                    self._get_tensor_n_elements(tensor),
                    self._get_tensor_dtype(tensor),
                    lingqu_op
                )
        lingqu_sdk.group_end()
    
    def send(self, tensor, options: SendOptions):
        """灵衢P2P发送
        
        利用灵衢特性：
        1. 支持跨节点零拷贝
        2. 内存共享直接传输
        """
        device = self._get_tensor_device(tensor)
        peer_rank = options.dst_rank
        peer_gpu_idx = options.dst_gpu_index
        
        comm_key = f"p2p_{self.rank}_{device}:{peer_rank}_{peer_gpu_idx}"
        comms = self._get_lingqu_p2p_communicator(comm_key, device, peer_rank)
        
        # 灵衢特有：使用内存共享发送
        if self._can_use_shared_memory(peer_rank):
            # 零拷贝内存共享传输
            memory_handle = self._create_lingqu_memory_handle(tensor)
            comms[0].send_shared_memory(
                memory_handle,
                peer_rank,
                self._get_tensor_n_elements(tensor)
            )
        else:
            # 通过灵衢总线传输
            comms[0].send_direct(
                self._get_tensor_ptr(tensor),
                self._get_tensor_n_elements(tensor),
                peer_rank
            )
    
    def recv(self, tensor, options: RecvOptions):
        """灵衢P2P接收"""
        device = self._get_tensor_device(tensor)
        src_rank = options.src_rank
        
        comm_key = f"p2p_{src_rank}:{self.rank}_{device}"
        comms = self._get_lingqu_p2p_communicator(comm_key, device, src_rank)
        
        # 接收逻辑
        if self._can_use_shared_memory(src_rank):
            # 从共享内存读取
            comms[0].recv_shared_memory(
                self._get_tensor_ptr(tensor),
                src_rank
            )
        else:
            comms[0].recv_direct(
                self._get_tensor_ptr(tensor),
                src_rank
            )
    
    def _create_lingqu_memory_handle(self, tensor):
        """创建灵衢内存共享句柄
        
        利用灵衢总线内存共享能力：
        1. 注册内存区域到灵衢总线
        2. 生成跨进程可访问的句柄
        3. 支持零拷贝传输
        """
        ptr = self._get_tensor_ptr(tensor)
        size = self._get_tensor_size(tensor)
        
        handle = LingquMemoryHandle(
            pointer=ptr,
            size=size,
            bus_id=self._get_lingqu_bus_id()
        )
        
        # 注册到灵衢总线
        lingqu_sdk.register_memory(handle)
        return handle
    
    def _can_use_shared_memory(self, peer_rank: int) -> bool:
        """判断是否可以使用灵衢内存共享
        
        条件：
        1. peer在同一节点
        2. 灵衢总线支持内存共享
        3. 内存区域已注册
        """
        # 获取peer节点ID
        peer_node_id = self._get_peer_node_id(peer_rank)
        my_node_id = ray.get_runtime_context().get_node_id()
        
        # 同节点且灵衢支持
        return (peer_node_id == my_node_id and 
                lingqu_sdk.supports_shared_memory())
    
    def destroy_group(self):
        """清理灵衢通信器"""
        for comm_key, comms in self._dev_comm_map.items():
            for comm in comms:
                comm.destroy()
        
        # 清理内存句柄
        for handle_key, handle in self._memory_handle_map.items():
            lingqu_sdk.unregister_memory(handle)
        
        self._dev_comm_map.clear()
        self._memory_handle_map.clear()
    
    # === 辅助方法 ===
    
    def _map_reduce_op(self, op: ReduceOp):
        """映射ReduceOp到灵衢操作"""
        op_map = {
            ReduceOp.SUM: lingqu_sdk.LINGQU_SUM,
            ReduceOp.PRODUCT: lingqu_sdk.LINGQU_PROD,
            ReduceOp.MIN: lingqu_sdk.LINGQU_MIN,
            ReduceOp.MAX: lingqu_sdk.LINGQU_MAX,
        }
        return op_map.get(op, lingqu_sdk.LINGQU_SUM)
    
    def _detect_tensor_location(self, tensor) -> str:
        """检测tensor位置（GPU/CPU）"""
        if hasattr(tensor, 'device'):
            if str(tensor.device).startswith('cuda'):
                return "GPU"
        return "CPU"
    
    def _get_tensor_ptr(self, tensor):
        """获取tensor内存指针"""
        if hasattr(tensor, 'data_ptr'):  # torch.Tensor
            return tensor.data_ptr()
        elif hasattr(tensor, 'data'):  # numpy/cupy
            return tensor.data
        raise TypeError(f"不支持的tensor类型: {type(tensor)}")
    
    def _get_tensor_devices(self, tensors: List) -> List[int]:
        """获取tensor所在的设备索引列表"""
        devices = []
        for t in tensors:
            if hasattr(t, 'device'):
                devices.append(t.device.index if t.device.type == 'cuda' else 0)
            else:
                devices.append(0)  # CPU默认
        return devices
```

---

#### 模块4：灵衢工具函数封装
**文件**：`python/ray/util/collective/collective_group/lingqu_util.py`（新建）

```python
"""灵衢总线API封装"""

import numpy
try:
    import lingqu_sdk
    from lingqu_sdk import (
        LingquCommunicator,
        get_version,
        get_build_version,
        generate_unique_id,
        register_memory,
        unregister_memory
    )
except ImportError:
    raise ImportError(
        "灵衢SDK未安装。请从灵衢厂商获取SDK并安装。"
    )

# 数据类型映射
NUMPY_LINGQU_DTYPE_MAP = {
    numpy.int8: lingqu_sdk.LINGQU_INT8,
    numpy.int32: lingqu_sdk.LINGQU_INT32,
    numpy.int64: lingqu_sdk.LINGQU_INT64,
    numpy.float16: lingqu_sdk.LINGQU_FLOAT16,
    numpy.float32: lingqu_sdk.LINGQU_FLOAT32,
    numpy.float64: lingqu_sdk.LINGQU_FLOAT64,
}

# 灵衢特有类型（支持混合精度）
LINGQU_SPECIAL_DTYPE_MAP = {
    "bf16": lingqu_sdk.LINGQU_BFLOAT16,  # 灵衢原生支持
    "tf32": lingqu_sdk.LINGQU_TFLOAT32,  # 灵衢加速模式
}


def get_lingqu_version():
    """获取灵衢SDK版本"""
    return get_version()


def create_lingqu_communicator(world_size, lingqu_uid, rank):
    """创建灵衢通信器"""
    return LingquCommunicator(world_size, lingqu_uid, rank)


def get_lingqu_unique_id():
    """生成灵衢UniqueId"""
    return generate_unique_id()


def register_lingqu_memory(ptr, size, bus_id):
    """注册内存到灵衢总线"""
    return register_memory(ptr, size, bus_id)


def get_lingqu_bus_topology():
    """获取灵衢总线拓扑信息
    
    返回：
    {
        "nodes": [...],
        "gpus_per_node": int,
        "memory_shared_regions": [...],
        "bandwidth": {...}  # 灵衢带宽信息
    }
    """
    return lingqu_sdk.get_bus_topology()


def check_lingqu_availability():
    """检查灵衢是否可用"""
    try:
        import lingqu_sdk
        return True
    except ImportError:
        return False


def get_tensor_ptr(tensor):
    """通用tensor指针获取"""
    if hasattr(tensor, 'data_ptr'):  # PyTorch
        return tensor.data_ptr()
    elif hasattr(tensor, 'data'):  # NumPy/CuPy
        return tensor.data
    raise ValueError(f"不支持的tensor类型: {type(tensor)}")
```

---

#### 模块5：GroupManager工厂方法修改
**文件**：`python/ray/util/collective/collective.py`

**修改内容**（Line 77-99）：
```python
# 原代码
def create_collective_group(self, backend, world_size, rank, group_name, gloo_timeout):
    backend = types.Backend(backend)
    if backend == types.Backend.GLOO:
        g = TorchGLOOGroup(world_size, rank, group_name, gloo_timeout)
    elif backend == types.Backend.NCCL:
        g = NCCLGroup(world_size, rank, group_name)
    else:
        raise RuntimeError(f"Unexpected backend: {backend}")

# 修改后
def create_collective_group(self, backend, world_size, rank, group_name, gloo_timeout):
    backend = types.Backend(backend)
    if backend == types.Backend.GLOO:
        g = TorchGLOOGroup(world_size, rank, group_name, gloo_timeout)
    elif backend == types.Backend.NCCL:
        g = NCCLGroup(world_size, rank, group_name)
    elif backend == types.Backend.LINGQU:  # 新增
        from ray.util.collective.collective_group.lingqu_collective_group import LingquGroup
        g = LingquGroup(world_size, rank, group_name)
    elif backend == types.Backend.LINGQU_SHM:  # 新增（内存共享模式）
        from ray.util.collective.collective_group.lingqu_collective_group import LingquGroup
        g = LingquGroup(world_size, rank, group_name, use_shared_memory=True)
    else:
        raise RuntimeError(f"Unexpected backend: {backend}")
    
    self._name_group_map[group_name] = g
    return g
```

---

#### 模块6：Tensor Transport扩展
**文件**：`python/ray/experimental/rdt/util.py`

**修改内容**（Line 87-111）：
```python
# 原代码
DEFAULT_TRANSPORTS = ["NIXL", "GLOO", "NCCL", "CUDA_IPC"]

# 修改后
DEFAULT_TRANSPORTS = ["NIXL", "GLOO", "NCCL", "CUDA_IPC", "LINGQU", "LINGQU_SHM"]

# 新增灵衢传输注册（Line 111附近）
TRANSPORT_REGISTRY = {
    # ... 原有注册
    "LINGQU": {
        "supported_devices": ["cpu", "cuda", "lingqu_gpu"],  # 支持混合
        "transport_class": LingquTransport,
        "tensor_type": [torch.Tensor, np.ndarray]
    },
    "LINGQU_SHM": {
        "supported_devices": ["cpu", "cuda"],
        "transport_class": LingquSharedMemoryTransport,
        "tensor_type": [torch.Tensor, np.ndarray]
    }
}
```

---

#### 模块7：新增灵衢Tensor Transport实现
**文件**：`python/ray/experimental/rdt/lingqu_transport.py`（新建）

```python
"""灵衢总线Tensor传输实现"""

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    TensorTransportManager,
    TensorTransportMetadata
)
from dataclasses import dataclass
from typing import List, Optional, Any

try:
    import lingqu_sdk
except ImportError:
    raise ImportError("灵衢SDK未安装")


@dataclass
class LingquTransportMetadata(TensorTransportMetadata):
    """灵衢传输元数据"""
    lingqu_memory_handle: Optional[Any] = None  # 灵衢内存句柄
    lingqu_bus_id: Optional[str] = None
    tensor_location: Optional[str] = None  # "CPU" or "GPU"
    is_shared_memory: bool = False


class LingquTransport(TensorTransportManager):
    """灵衢总线传输管理器"""
    
    def __init__(self):
        pass
    
    @property
    def tensor_transport_backend(self) -> str:
        return "LINGQU"
    
    @staticmethod
    def is_one_sided() -> bool:
        return True
    
    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        tensors: List
    ) -> LingquTransportMetadata:
        """提取tensor传输元数据
        
        灵衢特有：
        1. 自动检测CPU/GPU位置
        2. 创建内存共享句柄
        3. 记录总线拓扑信息
        """
        if not tensors:
            return LingquTransportMetadata()
        
        # 检测第一个tensor的位置
        first_tensor = tensors[0]
        tensor_location = self._detect_location(first_tensor)
        
        # 获取灵衢总线ID
        lingqu_bus_id = lingqu_sdk.get_current_bus_id()
        
        # 创建内存共享句柄（零拷贝）
        ptr = self._get_tensor_ptr(first_tensor)
        size = sum(t.numel() * t.element_size() for t in tensors)
        
        memory_handle = lingqu_sdk.create_memory_handle(
            ptr, size, lingqu_bus_id
        )
        
        return LingquTransportMetadata(
            lingqu_memory_handle=memory_handle,
            lingqu_bus_id=lingqu_bus_id,
            tensor_location=tensor_location,
            is_shared_memory=True  # 灵衢默认使用共享内存
        )
    
    def reconstruct_tensor(
        self,
        tensor_transport_metadata: LingquTransportMetadata,
        reader_actor_id: str
    ):
        """从灵衢共享内存重构tensor
        
        利用灵衢零拷贝特性直接访问发送方的内存
        """
        handle = tensor_transport_metadata.lingqu_memory_handle
        bus_id = tensor_transport_metadata.lingqu_bus_id
        
        # 从灵衢总线获取共享内存指针
        shared_ptr = lingqu_sdk.access_shared_memory(handle, bus_id)
        
        # 根据tensor_location选择重构方式
        if tensor_transport_metadata.tensor_location == "GPU":
            # GPU tensor，直接映射到接收方GPU
            import torch
            tensor = torch.from_pointer(
                shared_ptr,
                shape=tensor_transport_metadata.tensor_shape,
                dtype=tensor_transport_metadata.tensor_dtype,
                device=f"cuda:{ray.get_gpu_ids()[0]}"
            )
        else:
            # CPU tensor，零拷贝访问
            import numpy as np
            tensor = np.frombuffer(
                shared_ptr,
                dtype=tensor_transport_metadata.numpy_dtype
            )
        
        return tensor
    
    def _detect_location(self, tensor) -> str:
        """检测tensor位置"""
        if hasattr(tensor, 'device'):
            if 'cuda' in str(tensor.device):
                return "GPU"
        return "CPU"
    
    def _get_tensor_ptr(self, tensor):
        """获取tensor指针"""
        if hasattr(tensor, 'data_ptr'):
            return tensor.data_ptr()
        return tensor.data


class LingquSharedMemoryTransport(LingquTransport):
    """灵衢共享内存传输（优化版本）"""
    
    @property
    def tensor_transport_backend(self) -> str:
        return "LINGQU_SHM"
    
    def extract_tensor_transport_metadata(self, obj_id, tensors):
        """强制使用共享内存模式"""
        meta = super().extract_tensor_transport_metadata(obj_id, tensors)
        meta.is_shared_memory = True
        
        # 灵衢特有：注册到全局内存池
        lingqu_sdk.register_to_global_pool(meta.lingqu_memory_handle)
        
        return meta
```

---

### 2.2 C++层核心修改（共5个模块）

#### 模块8：Protobuf消息扩展
**文件1**：`src/ray/protobuf/common.proto`

**修改位置**：Line 622附近
```protobuf
// 原代码
optional string tensor_transport = 44;

// 修改后
optional string tensor_transport = 44;
// 新增：灵衢传输选项
enum TensorTransportType {
  OBJECT_STORE = 0;
  NCCL = 1;
  GLOO = 2;
  CUDA_IPC = 3;
  LINGQU = 4;        // 新增
  LINGQU_SHM = 5;    // 新增
}
optional TensorTransportType tensor_transport_type = 45;
```

**文件2**：`src/ray/protobuf/core_worker.proto`
```protobuf
// Line 76附近
bool enable_tensor_transport = 16;
// 新增
optional string lingqu_bus_id = 17;  // 灵衢总线ID
optional bool use_lingqu_shared_memory = 18;  // 是否使用灵衢共享内存
```

---

#### 模块9：TaskSpecification扩展
**文件**：`src/ray/common/task/task_spec.h` + `task_spec.cc`

**新增方法**（Line 186附近）：
```cpp
// task_spec.h
std::optional<std::string> TensorTransport() const;

// 新增灵衢相关方法
std::optional<std::string> LingquBusId() const;
bool UseLingquSharedMemory() const;

// task_spec.cc (Line 498附近实现)
std::optional<std::string> TaskSpecification::TensorTransport() const {
    if (message_->has_tensor_transport()) {
        return message_->tensor_transport();
    }
    return std::nullopt;
}

std::optional<std::string> TaskSpecification::LingquBusId() const {
    if (message_->has_lingqu_bus_id()) {
        return message_->lingqu_bus_id();
    }
    return std::nullopt;
}

bool TaskSpecification::UseLingquSharedMemory() const {
    return message_->use_lingqu_shared_memory();
}
```

---

#### 模块10：CoreWorker GPU资源管理适配
**文件**：`src/ray/core_worker/core_worker.h` + `core_worker.cc`

**修改内容**：
```cpp
// core_worker.h (Line 526附近)
/// \param[in] tensor_transport The tensor transport to use for the object.
/// \param[in] lingqu_bus_id 灵衢总线ID（新增）
Status PutObject(
    const RayObject &object,
    const ObjectID &object_id,
    const std::optional<std::string> &tensor_transport = std::nullopt,
    const std::optional<std::string> &lingqu_bus_id = std::nullopt  // 新增
);

// core_worker.cc (Line 1047附近实现)
Status CoreWorker::PutObject(
    const RayObject &object,
    const ObjectID &object_id,
    const std::optional<std::string> &tensor_transport,
    const std::optional<std::string> &lingqu_bus_id
) {
    // 原有逻辑
    reference_counter_->AddObjectReference(
        object_id, owner_address, 
        /*tensor_transport=*/tensor_transport
    );
    
    // 新增：灵衢内存注册逻辑
    if (tensor_transport && *tensor_transport == "LINGQU") {
        // 调用灵衢SDK（通过C binding）
        lingqu_register_memory(
            object.GetData()->Data(),
            object.GetData()->Size(),
            lingqu_bus_id->c_str()
        );
    }
    
    return store_provider_->Put(object, object_id);
}
```

---

#### 模块11：Memory Store适配灵衢共享内存
**文件**：`src/ray/core_worker/store_provider/memory_store/memory_store.cc`

**修改位置**：Line 185附近
```cpp
// 原代码
Status MemoryStore::Put(..., object.GetTensorTransport())

// 新增灵衢共享内存检查
Status MemoryStore::Put(
    const std::shared_ptr<RayObject> &object,
    const ObjectID &object_id
) {
    auto tensor_transport = object.GetTensorTransport();
    
    // 灵衢共享内存特殊处理
    if (tensor_transport == "LINGQU_SHM") {
        // 零拷贝：直接注册到灵衢总线
        void* ptr = object->GetData()->Data();
        size_t size = object->GetData()->Size();
        
        // 调用灵衢C SDK注册
        lingqu_memory_handle_t handle = lingqu_register_shared_memory(
            ptr, size, object->GetLingquBusId()
        );
        
        // 存储句柄而非数据拷贝
        objects_[object_id] = LingquMemoryObject{
            .handle = handle,
            .bus_id = object->GetLingquBusId()
        };
        
        return Status::OK();
    }
    
    // 原有逻辑（非灵衢）
    return PutInternal(object, object_id);
}
```

---

#### 模块12：新增灵衢C SDK Binding
**文件**：`src/ray/core_worker/lingqu/lingqu_binding.h`（新建）

```cpp
/** 灵衢总线C++接口封装 */

#ifndef RAY_LINGQU_BINDING_H
#define RAY_LINGQU_BINDING_H

#include <string>
#include <cstdint>
#include "ray/status.h"

namespace ray {
namespace lingqu {

// 灵衢内存句柄结构
struct LingquMemoryHandle {
    void* pointer;
    size_t size;
    std::string bus_id;
    uint64_t handle_id;
};

// 灵衢通信器封装
class LingquCommunicator {
public:
    LingquCommunicator(int world_size, const std::string& unique_id, int rank);
    ~LingquCommunicator();
    
    // 集体通信操作
    Status AllReduce(void* sendbuf, void* recvbuf, size_t count, 
                     int dtype, int op);
    Status AllGather(void* sendbuf, void* recvbuf, size_t count);
    Status Broadcast(void* buf, size_t count, int root);
    Status Send(void* buf, size_t count, int dst_rank);
    Status Recv(void* buf, size_t count, int src_rank);
    
    // 灵衢特有：混合通信
    Status HybridAllReduce(void* cpu_buf, void* gpu_buf, size_t count);
    
    // 内存共享操作
    Status RegisterSharedMemory(void* ptr, size_t size, const std::string& bus_id);
    Status AccessSharedMemory(uint64_t handle_id, void** out_ptr);
    
private:
    void* communicator_;  // 灵衢SDK内部通信器指针
    int world_size_;
    int rank_;
};

// 灵衢SDK调用接口
std::string GenerateUniqueId();
Status RegisterMemory(void* ptr, size_t size, const std::string& bus_id);
Status UnregisterMemory(uint64_t handle_id);

// 灵衢拓扑查询
struct LingquTopology {
    int num_nodes;
    int gpus_per_node;
    std::string bus_type;
    uint64_t bandwidth;  // 灵衢带宽（GB/s）
};

LingquTopology GetLingquTopology();

// 灵衢SDK版本
std::string GetLingquVersion();

}  // namespace lingqu
}  // namespace ray

// C binding（供Python调用）
extern "C" {
    void* lingqu_create_communicator(int world_size, const char* uid, int rank);
    void lingqu_destroy_communicator(void* comm);
    
    int lingqu_all_reduce(void* comm, void* sendbuf, void* recvbuf,
                          size_t count, int dtype, int op);
    
    uint64_t lingqu_register_memory(void* ptr, size_t size, const char* bus_id);
    int lingqu_unregister_memory(uint64_t handle_id);
    
    char* lingqu_generate_unique_id();
    char* lingqu_get_version();
}

#endif  // RAY_LINGQU_BINDING_H
```

---

### 2.3 编译配置修改

#### 模块13：CMakeLists.txt扩展
**文件**：`CMakeLists.txt`（根目录）

```cmake
# 原有内容...

# 新增灵衢SDK配置
option(RAY_USE_LINGQU "启用灵衢总线支持" OFF)

if(RAY_USE_LINGQU)
    # 查找灵衢SDK
    find_package(LingquSDK REQUIRED)
    
    if(LingquSDK_FOUND)
        message(STATUS "找到灵衢SDK: ${LingquSDK_VERSION}")
        message(STATUS "灵衢SDK路径: ${LingquSDK_INCLUDE_DIR}")
        
        # 添加编译选项
        add_definitions(-DRAY_USE_LINGQU=1)
        
        # 包含目录
        include_directories(${LingquSDK_INCLUDE_DIR})
        
        # 链接库
        link_libraries(${LingquSDK_LIBRARY})
        
        # 编译灵衢相关源文件
        list(APPEND RAY_CORE_WORKER_SOURCES
            src/ray/core_worker/lingqu/lingqu_binding.cc
            src/ray/core_worker/lingqu/lingqu_memory_manager.cc
        )
    else()
        message(WARNING "未找到灵衢SDK，将禁用灵衢支持")
        set(RAY_USE_LINGQU OFF)
    endif()
endif()
```

---

## 三、实现优先级与依赖关系

### 3.1 实施阶段规划

```
阶段1：基础适配层（1-2周）
├── Backend类型扩展 (types.py)
├── Protobuf消息扩展 (common.proto)
└── 编译配置 (CMakeLists.txt)

阶段2：核心通信层（2-3周）
├── LingquGroup实现 (lingqu_collective_group.py)
├── Lingqu工具函数 (lingqu_util.py)
├── GroupManager工厂修改 (collective.py)
└── 灵衢C++ Binding (lingqu_binding.h/cc)

阶段3：内存共享层（2-3周）
├── LingquTransport实现 (lingqu_transport.py)
├── Memory Store适配 (memory_store.cc)
├── CoreWorker适配 (core_worker.cc)
└── 灵衢内存管理器 (lingqu_memory_manager.cc)

阶段4：优化与测试（2-4周）
├── 性能优化（零拷贝路径）
├── 单元测试
├── 多节点集成测试
└── 性能基准测试

阶段5：文档与部署（1-2周）
├── 用户文档
├── 部署指南
├── 示例代码
└── 官方支持声明
```

### 3.2 关键依赖关系图

```
types.py (Backend定义)
    ↓
lingqu_util.py (基础工具)
    ↓
lingqu_collective_group.py (通信组)
    ↓
collective.py (GroupManager工厂)
    ↓
用户API层 (init_collective_group)

lingqu_binding.h (C++接口)
    ↓
core_worker.cc (Worker适配)
    ↓
memory_store.cc (存储适配)
    ↓
Python层调用 (通过pybind11)

CMakeLists.txt (编译配置)
    ↓
Protobuf定义
    ↓
所有C++模块编译
```

---

## 四、关键接口实现清单

### 4.1 必须实现的抽象接口

| 接口类别 | 接口方法 | 实现位置 | 优先级 |
|---------|---------|---------|--------|
| **集体通信** | allreduce | LingquGroup | P0 |
| | allgather | LingquGroup | P0 |
| | broadcast | LingquGroup | P1 |
| | reduce | LingquGroup | P1 |
| | reducescatter | LingquGroup | P2 |
| | send/recv (P2P) | LingquGroup | P0 |
| | barrier | LingquGroup | P1 |
| **内存共享** | register_memory | lingqu_binding.h | P0 |
| | unregister_memory | lingqu_binding.h | P0 |
| | access_shared_memory | lingqu_binding.h | P0 |
| **传输层** | extract_metadata | LingquTransport | P1 |
| | reconstruct_tensor | LingquTransport | P1 |
| **拓扑查询** | get_bus_topology | lingqu_util.py | P2 |
| **版本管理** | get_version | lingqu_util.py | P0 |

### 4.2 灵衢SDK需要提供的功能

**Python SDK接口（lingqu_sdk模块）**：
```python
# 必须提供
class LingquCommunicator:
    def __init__(world_size, unique_id, rank, bus_type)
    def all_reduce_gpu(sendbuf, recvbuf, count, dtype, op)
    def all_reduce_hybrid(sendbuf, recvbuf, count, dtype, op)
    def send_direct(buf, count, peer_rank)
    def send_shared_memory(handle, peer_rank)
    def destroy()

def generate_unique_id() -> str
def register_memory(ptr, size, bus_id) -> LingquMemoryHandle
def unregister_memory(handle)
def get_version() -> str
def supports_shared_memory() -> bool
def get_bus_topology() -> dict

# 常量定义
LINGQU_SUM, LINGQU_PROD, LINGQU_MIN, LINGQU_MAX
LINGQU_INT8, LINGQU_INT32, LINGQU_INT64
LINGQU_FLOAT16, LINGQU_FLOAT32, LINGQU_FLOAT64
LINGQU_BFLOAT16, LINGQU_TFLOAT32
```

**C SDK接口（供Ray C++层调用）**：
```c
// 必须提供
void* lingqu_create_communicator(int world_size, char* uid, int rank)
void lingqu_destroy_communicator(void* comm)

int lingqu_all_reduce(void* comm, void* send, void* recv, 
                      size_t count, int dtype, int op)

uint64_t lingqu_register_memory(void* ptr, size_t size, char* bus_id)
int lingqu_unregister_memory(uint64_t handle_id)
void* lingqu_access_shared_memory(uint64_t handle_id)

char* lingqu_generate_unique_id()
char* lingqu_get_version()
int lingqu_supports_shared_memory()

// 拓扑查询
char* lingqu_get_bus_topology_json()
```

---

## 五、测试与验证方案

### 5.1 单元测试清单

| 测试模块 | 测试文件 | 关键测试项 |
|---------|---------|-----------|
| Backend | `test_lingqu_backend.py` | Backend.LINGQU识别、错误处理 |
| LingquGroup | `test_lingqu_collective.py` | allreduce正确性、P2P通信、混合设备 |
| LingquTransport | `test_lingqu_transport.py` | 内存共享、零拷贝、重构tensor |
| Memory Store | `test_lingqu_memory_store.cc` | 共享内存存储、句柄管理 |

### 5.2 集成测试场景

```python
# 场景1：单节点多GPU AllReduce
@ray.remote(num_gpus=1)
class Worker:
    def test_allreduce(self):
        init_collective_group(4, rank, backend="LINGQU")
        tensor = torch.randn(1000, 1000, device="cuda")
        allreduce(tensor)
        return tensor

# 场景2：跨节点内存共享
@ray.remote(num_gpus=1)
class Sender:
    def send_tensor(self):
        # 创建灵衢共享内存tensor
        tensor = torch.randn(1000, device="cuda")
        # 通过灵衢总线零拷贝传输
        return tensor.with_tensor_transport(transport="LINGQU_SHM")

@ray.remote(num_gpus=1)
class Receiver:
    def recv_tensor(self, tensor_handle):
        # 直接访问发送方内存
        return process(tensor_handle)

# 场景3：CPU-GPU混合通信
@ray.remote
class CPUWorker:
    def compute(self):
        # CPU tensor通过灵衢总线传到GPU
        data = np.random.randn(1000)
        return data.with_tensor_transport(transport="LINGQU")

@ray.remote(num_gpus=1)
class GPUWorker:
    def process(self, data_handle):
        # GPU端接收CPU数据
        tensor = torch.from_numpy(data_handle).cuda()
        return tensor
```

### 5.3 性能基准测试

| 测试项 | 对比基准 | 期望性能提升 |
|-------|---------|-------------|
| GPU-to-GPU AllReduce | NCCL + NVLink | ≥1.2x（灵衢带宽优势） |
| CPU-to-GPU传输 | PCIe + Object Store | ≥2x（零拷贝） |
| 内存共享传输 | CUDA IPC | ≥1.5x（灵衢共享内存） |
| 多节点AllReduce | NCCL + Network | ≥3x（灵衢超节点） |

---

## 六、部署与配置指南

### 6.1 环境要求

```yaml
硬件：
  - 灵衢超节点服务器集群
  - 灵衢总线连接的CPU/GPU节点
  - 灵衢SDK版本 >= 1.0.0

软件：
  - Ray >= 2.55.1（修改版）
  - 灵衢SDK Python包（lingqu-sdk）
  - 灵衢SDK C库（lingqu_core.so）
  - PyTorch >= 2.0（可选）
  - Python >= 3.8
```

### 6.2 安装步骤

```bash
# 步骤1：安装灵衢SDK
pip install lingqu-sdk  # 从灵衢厂商获取

# 步骤2：编译Ray（启用灵衢支持）
cd ray-ray-2.55.1
mkdir build && cd build
cmake .. -DRAY_USE_LINGQU=ON -DLingquSDK_DIR=/path/to/lingqu/sdk
make -j8
make install

# 步骤3：安装修改后的Ray Python包
cd ../python
pip install -e .

# 步骤4：验证安装
python -c "import ray; print(ray.util.collective.types.Backend.LINGQU)"
python -c "import lingqu_sdk; print(lingqu_sdk.get_version())"
```

### 6.3 配置示例

```python
# Ray启动配置（启用灵衢）
ray.init(
    num_gpus=4,
    _system_config={
        "enable_lingqu_support": True,
        "lingqu_bus_id": "lingqu_bus_0",  # 灵衢总线ID
        "lingqu_memory_pool_size": 1024 * 1024 * 1024,  # 1GB共享内存池
    }
)

# 创建灵衢通信组
from ray.util.collective import init_collective_group

@ray.remote(num_gpus=1)
class LingquWorker:
    def __init__(self, rank):
        # 使用灵衢backend
        init_collective_group(
            world_size=4,
            rank=rank,
            backend="LINGQU",  # 指定灵衢
            group_name="lingqu_group"
        )
    
    def compute(self, data):
        # 灵衢自动处理GPU/CPU混合通信
        import torch
        allreduce(data)  # 自动选择最优路径
        return data

# 启动workers
workers = [LingquWorker.remote(i) for i in range(4)]
```

---

## 七、注意事项与风险

### 7.1 兼容性风险

| 风险点 | 影响 | 缓解措施 |
|-------|------|---------|
| 灵衢SDK版本不兼容 | 功能缺失或崩溃 | 严格版本检查，兼容性测试 |
| 混合设备路径选择错误 | 性能下降 | 自动拓扑检测，手动指定路径 |
| 内存共享句柄泄漏 | 内存耗尽 | 引用计数管理，自动清理 |
| 灵衢总线拓扑变化 | 通信失败 | 动态拓扑检测，热重载机制 |

### 7.2 性能优化建议

```
优化点1：内存池预分配
- 启动时分配灵衢共享内存池
- 避免频繁注册/注销内存

优化点2：拓扑感知调度
- 根据灵衢拓扑自动分配tasks
- 同节点优先使用内存共享

优化点3：混合路径自动选择
- CPU tensor → 灵衢总线 → GPU（零拷贝）
- GPU tensor → 灵衢总线 → GPU（直接传输）
- 跨节点 → 灵衢超节点网络（高带宽）

优化点4：通信重叠（类似NCCL）
- 支持通信与计算流水线
- 利用灵衢异步传输特性
```

---

## 八、总结

### 8.1 改造规模评估

| 改造类别 | 文件数量 | 代码行数估算 | 工作量 |
|---------|---------|-------------|--------|
| Python新增 | 3个新文件 | ~1200行 | 2-3周 |
| Python修改 | 3个文件 | ~100行 | 1周 |
| C++新增 | 2个新文件 | ~800行 | 2周 |
| C++修改 | 5个文件 | ~150行 | 1周 |
| Protobuf | 2个文件 | ~30行 | 1天 |
| 编译配置 | 1个文件 | ~20行 | 1天 |
| **总计** | **15个文件** | **~2300行** | **6-8周** |

### 8.2 关键成功因素

1. **灵衢SDK质量**：稳定的Python/C接口，完善的文档
2. **灵衢硬件稳定性**：总线拓扑一致，内存共享可靠
3. **测试覆盖度**：充分的单元测试和集成测试
4. **性能验证**：与NCCL对比，达到预期性能提升
5. **文档完善**：用户指南、API文档、示例代码

### 8.3 建议实施顺序

```
验证阶段（1周）
└─ 灵衢SDK功能验证（Python + C接口测试）

基础阶段（2周）
├─ Backend类型扩展 + Protobuf
├─ 灵衢工具函数实现
└─ GroupManager工厂修改

核心阶段（3周）
├─ LingquGroup集体通信实现
├─ LingquTransport传输实现
└─ C++ Binding + Memory Store适配

优化阶段（2周）
├─ 性能优化（零拷贝路径）
├─ 拓扑感知调度
└─ 通信重叠实现

测试阶段（2周）
├─ 单元测试 + 集成测试
├─ 性能基准测试
└─ 多节点部署测试

发布阶段（1周）
├─ 文档编写
├─ 示例代码
└─ 发布准备
```

---

## 九、参考资料

1. **Ray源码位置索引**：
   - Backend定义：`python/ray/util/collective/types.py:34-52`
   - BaseGroup抽象：`python/ray/util/collective/collective_group/base_collective_group.py:16-85`
   - NCCL实现：`python/ray/util/collective/collective_group/nccl_collective_group.py:121-836`
   - Tensor Transport：`python/ray/experimental/rdt/`
   - Protobuf：`src/ray/protobuf/common.proto`, `core_worker.proto`

2. **灵衢SDK接口需求**：见第四章4.2节

3. **测试方案**：见第五章完整测试清单

---

**文档版本**：v1.0
**生成日期**：2026-05-18
**适用Ray版本**：2.55.1
**预估开发周期**：6-8周（包含测试与优化）