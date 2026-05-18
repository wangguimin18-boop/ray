# Ray RDT 集成华为灵衢总线设计方案

## 1. 概述

### 1.1 背景
Ray Direct Transport (RDT) 是 Ray 的 GPU 数据直通特性，目前支持以下传输方式：
- **NIXL**: NVIDIA GPUDirect RDMA + UCX
- **NCCL**: NVIDIA 集合通信
- **CUDA IPC**: 同节点 CUDA IPC
- **Gloo**: CPU 集合通信

华为灵衢总线提供了类似 NVIDIA GPUDirect 的 GPU 直通技术，需要将其集成到 Ray RDT 中。

### 1.2 技术对比

| 特性 | NVIDIA GPUDirect/NIXL | 华为灵衢总线 |
|------|----------------------|-------------|
| RDMA支持 | Yes (通过UCX) | Yes |
| GPU内存注册 | nixl_agent.register_memory | 需对应API |
| 跨节点传输 | Yes | Yes |
| 单边传输 | Yes | 需确认 |
| 内存类型 | cuda/cpu | npu/cpu |

## 2. 架构分析

### 2.1 Ray RDT 核心接口

```
python/ray/experimental/rdt/
├── tensor_transport_manager.py   # 抽象基类 TensorTransportManager
├── nixl_tensor_transport.py      # NIXL实现（参考模板）
├── collective_tensor_transport.py # NCCL/Gloo实现
├── cuda_ipc_transport.py         # CUDA IPC实现
├── rdt_manager.py                # RDT管理器
├── rdt_store.py                  # RDT存储
├── util.py                       # 工具函数和注册机制
└── __init__.py                   # 导出接口
```

### 2.2 TensorTransportManager 抽象接口

```python
class TensorTransportManager(ABC):
    @abstractmethod
    def tensor_transport_backend(self) -> str          # 返回backend名称
    
    @staticmethod
    @abstractmethod
    def is_one_sided() -> bool                         # 是否单边传输
    
    @staticmethod
    @abstractmethod
    def can_abort_transport() -> bool                  # 是否可中断
    
    @abstractmethod
    def actor_has_tensor_transport(self, actor) -> bool
    
    @abstractmethod
    def extract_tensor_transport_metadata(...)          # 提取传输元数据
    
    @abstractmethod
    def get_communicator_metadata(...)                  # 获取通信器元数据
    
    @abstractmethod
    def recv_multiple_tensors(...)                      # 接收张量
    
    @abstractmethod
    def send_multiple_tensors(...)                      # 发送张量
    
    @abstractmethod
    def garbage_collect(...)                            # 垃圾回收
    
    @abstractmethod
    def abort_transport(...)                            # 中断传输
```

## 3. 设计方案

### 3.1 整体方案选择

**方案A: 完全新增 LINGQU backend（推荐）**
- 新建 `lingqu_tensor_transport.py`
- 实现独立的 TensorTransportManager 子类
- 通过 `register_tensor_transport` 注册

**方案B: 扩展 NIXL**
- 不推荐，因为底层API差异较大

### 3.2 新增文件清单

```
python/ray/experimental/rdt/
├── lingqu_tensor_transport.py    # [新增] 灵衢传输实现
└── __init__.py                   # [修改] 导出新接口

python/ray/experimental/
└── __init__.py                   # [修改] 导出注册函数

doc/source/ray-core/
├── direct-transport.rst          # [修改] 添加灵衢文档
└── api/
    └── direct-transport.rst      # [修改] API文档
```

### 3.3 修改文件清单

| 文件 | 修改内容 |
|------|---------|
| `python/ray/experimental/rdt/util.py` | 添加LINGQU到DEFAULT_TRANSPORTS |
| `python/ray/experimental/rdt/__init__.py` | 导出 `register_lingqu_memory` |
| `python/ray/experimental/__init__.py` | 导出新接口 |
| `doc/source/ray-core/direct-transport.rst` | 添加灵衢使用说明 |
| `python/ray/_private/ray_constants.py` | 可选：添加灵衢相关常量 |

## 4. 代码实现框架

### 4.1 lingqu_tensor_transport.py 完整框架

```python
"""
华为灵衢总线 Tensor Transport 实现

文件位置: python/ray/experimental/rdt/lingqu_tensor_transport.py
"""

import threading
import time
import traceback
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray._private.ray_constants import LINGQU_REMOTE_AGENT_CACHE_MAXSIZE
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch


@dataclass
class LingquCommunicatorMetadata(CommunicatorMetadata):
    """灵衢通信器元数据"""
    # 根据灵衢API需求扩展字段
    pass


@dataclass
class LingquTransportMetadata(TensorTransportMetadata):
    """灵衢传输元数据
    
    Args:
        lingqu_serialized_descs: 序列化的张量描述符
        lingqu_agent_meta: 远程灵衢Agent元数据
        lingqu_agent_name: 灵衢Agent名称
        lingqu_agent_meta_version: Agent元数据版本号
    """
    lingqu_serialized_descs: Optional[bytes] = None
    lingqu_agent_meta: Optional[bytes] = None
    lingqu_agent_name: Optional[str] = None
    lingqu_agent_meta_version: Optional[int] = 0

    __eq__ = object.__eq__
    __hash__ = object.__hash__


@dataclass
class TensorDesc:
    """张量描述符缓存"""
    reg_desc: Any  # 灵衢注册描述符
    metadata_count: int  # 引用计数


class LingquTensorTransport(TensorTransportManager):
    """华为灵衢总线传输实现
    
    参考 NIXL 实现，适配灵衢 API
    """
    
    def __init__(self):
        # 懒加载，因为需要灵衢库已安装
        self._lingqu_agent = None
        self._aborted_transfer_obj_ids = set()
        self._aborted_transfer_obj_ids_lock = threading.Lock()
        
        # 张量描述符缓存
        self._tensor_desc_cache: Dict[int, TensorDesc] = {}
        
        # 管理的元数据
        self._managed_meta_lingqu: Dict[str, Any] = {}
        
        # 缓存锁
        self._cache_lock = threading.RLock()
        
        # 远程Agent LRU缓存
        self._remote_agents: OrderedDict = OrderedDict()
        
        # 元数据版本号
        self._lingqu_agent_meta_version = 0

    def tensor_transport_backend(self) -> str:
        return "LINGQU"

    @staticmethod
    def is_one_sided() -> bool:
        """灵衢是否支持单边传输，需根据实际API确认"""
        return True  # 需根据灵衢特性确认

    @staticmethod
    def can_abort_transport() -> bool:
        """是否可中断传输"""
        return True  # 需根据灵衢特性确认

    def register_lingqu_memory(self, tensor: "torch.Tensor") -> None:
        """注册张量内存到灵衢"""
        self._add_tensor_descs([tensor])

    def get_lingqu_agent(self):
        """创建灵衢Agent
        
        需替换为实际的灵衢SDK API调用
        
        示例（需根据实际SDK调整）:
        ```python
        from lingqu_sdk import lingqu_agent, lingqu_agent_config
        
        agent_config = lingqu_agent_config(backends=["RDMA"])
        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        if actor_id is None:
            actor_id = f"RAY-DRIVER-{uuid.uuid4()}"
        self._lingqu_agent = lingqu_agent(actor_id, agent_config)
        return self._lingqu_agent
        ```
        """
        if self._lingqu_agent is not None:
            return self._lingqu_agent
        
        # ========================================
        # TODO: 替换为实际灵衢SDK初始化代码
        # ========================================
        try:
            # 示例导入，需替换为实际灵衢SDK
            from lingqu import lingqu_agent, lingqu_agent_config
            
            agent_config = lingqu_agent_config(backends=["RDMA"])
            ctx = ray.get_runtime_context()
            actor_id = ctx.get_actor_id()
            if actor_id is None:
                import uuid
                actor_id = f"RAY-DRIVER-{uuid.uuid4()}"
            self._lingqu_agent = lingqu_agent(actor_id, agent_config)
            return self._lingqu_agent
        except ImportError:
            raise RuntimeError(
                "华为灵衢SDK未安装。请安装灵衢相关依赖后使用。"
            )

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        """检查Actor是否有灵衢传输能力"""
        def __ray_actor_has_tensor_transport__(self):
            try:
                from ray.experimental.rdt.util import get_tensor_transport_manager
                get_tensor_transport_manager("LINGQU").get_lingqu_agent()
                return True
            except Exception:
                return False
        
        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                __ray_actor_has_tensor_transport__
            )
        )

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List["torch.Tensor"],
    ) -> LingquTransportMetadata:
        """提取传输元数据
        
        关键步骤:
        1. 检查张量设备和连续性
        2. GPU同步确保数据就绪
        3. 注册内存到灵衢
        4. 获取传输描述符
        5. 获取Agent元数据
        """
        import torch
        
        with self._cache_lock:
            device = None
            tensor_meta = []
            
            if rdt_object:
                devices = set()
                device = rdt_object[0].device
                for t in rdt_object:
                    # 校验张量属性
                    if t.device.type != device.type:
                        raise ValueError(
                            "RDT对象中的所有张量必须是相同设备类型"
                        )
                    if not t.is_contiguous():
                        raise ValueError(
                            "RDT对象中的所有张量必须是连续的"
                        )
                    tensor_meta.append((t.shape, t.dtype))
                    devices.add(t.device)
                
                # NPU同步（假设华为NPU设备类型为 "npu"）
                if device.type == "npu":  # 或 "cuda" 如果使用华为GPU
                    for dev in devices:
                        torch.npu.synchronize(dev)  # 需根据实际API调整
                
                # ========================================
                # TODO: 灵衢内存注册和描述符获取
                # ========================================
                lingqu_agent = self.get_lingqu_agent()
                self._add_tensor_descs(rdt_object)
                
                # 获取传输描述符（需替换为实际灵衢API）
                xfer_descs = lingqu_agent.get_xfer_descs(rdt_object)
                serialized_descs = lingqu_agent.get_serialized_descs(xfer_descs)
                agent_meta = lingqu_agent.get_agent_metadata()
                agent_name = lingqu_agent.name
                agent_meta_version = self._lingqu_agent_meta_version
            else:
                serialized_descs, agent_meta = None, None
                agent_name, agent_meta_version = None, None
            
            ret = LingquTransportMetadata(
                tensor_meta=tensor_meta,
                tensor_device=device.type if device else None,
                lingqu_serialized_descs=serialized_descs,
                lingqu_agent_meta=agent_meta,
                lingqu_agent_name=agent_name,
                lingqu_agent_meta_version=agent_meta_version,
            )
            self._put_meta(obj_id, ret)
            return ret

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> LingquCommunicatorMetadata:
        """获取通信器元数据"""
        return LingquCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> List["torch.Tensor"]:
        """接收张量
        
        关键步骤:
        1. 创建或使用目标缓冲区
        2. 添加远程Agent
        3. 初始化传输
        4. 执行RDMA读取
        5. 等待传输完成
        """
        from ray.experimental.rdt.util import create_empty_tensors_from_metadata
        
        tensors = target_buffers or create_empty_tensors_from_metadata(
            tensor_transport_metadata
        )
        
        assert isinstance(tensor_transport_metadata, LingquTransportMetadata)
        assert isinstance(communicator_metadata, LingquCommunicatorMetadata)
        
        lingqu_serialized_descs = tensor_transport_metadata.lingqu_serialized_descs
        remote_lingqu_agent_meta = tensor_transport_metadata.lingqu_agent_meta
        
        with self._aborted_transfer_obj_ids_lock:
            if obj_id in self._aborted_transfer_obj_ids:
                self._aborted_transfer_obj_ids.remove(obj_id)
                raise RuntimeError(f"灵衢传输中断: {obj_id}")
        
        if not tensors:
            return []
        
        local_xfer_descs = None
        remote_name = None
        xfer_handle = None
        added_tensor_descs = False
        
        try:
            lingqu_agent = self.get_lingqu_agent()
            
            # ========================================
            # TODO: 灵衢传输执行
            # ========================================
            
            # 反序列化远程描述符
            remote_xfer_descs = lingqu_agent.deserialize_descs(lingqu_serialized_descs)
            
            # 注册本地张量
            self._add_tensor_descs(tensors)
            added_tensor_descs = True
            local_xfer_descs = lingqu_agent.get_xfer_descs(tensors)
            
            remote_name = tensor_transport_metadata.lingqu_agent_name
            remote_agent_meta_version = tensor_transport_metadata.lingqu_agent_meta_version
            
            # 远程Agent缓存管理
            if LINGQU_REMOTE_AGENT_CACHE_MAXSIZE > 0:
                if remote_name in self._remote_agents:
                    if remote_agent_meta_version != self._remote_agents[remote_name]:
                        lingqu_agent.remove_remote_agent(remote_name)
                    self._remote_agents.move_to_end(remote_name)
                elif len(self._remote_agents) >= LINGQU_REMOTE_AGENT_CACHE_MAXSIZE:
                    evicted_agent_name, _ = self._remote_agents.popitem(last=False)
                    lingqu_agent.remove_remote_agent(evicted_agent_name)
                self._remote_agents[remote_name] = remote_agent_meta_version
            
            # 添加远程Agent
            lingqu_agent.add_remote_agent(remote_lingqu_agent_meta)
            
            # 初始化传输
            xfer_handle = lingqu_agent.initialize_xfer(
                "READ",
                local_xfer_descs,
                remote_xfer_descs,
                remote_name,
                b"UUID",
            )
            
            # 执行传输
            state = lingqu_agent.transfer(xfer_handle)
            if state == "ERR":
                raise RuntimeError("灵衢传输进入错误状态")
            
            # 等待完成
            while True:
                state = lingqu_agent.check_xfer_state(xfer_handle)
                if state == "ERR":
                    raise RuntimeError("灵衢传输进入错误状态")
                if state == "PROC":
                    with self._aborted_transfer_obj_ids_lock:
                        if obj_id in self._aborted_transfer_obj_ids:
                            self._aborted_transfer_obj_ids.remove(obj_id)
                            raise RuntimeError(f"灵衢传输中断: {obj_id}")
                    time.sleep(0.001)
                elif state == "DONE":
                    break
        
        except Exception:
            from ray.exceptions import RayDirectTransportError
            raise RayDirectTransportError(
                f"灵衢接收失败: {obj_id}\n{traceback.format_exc()}"
            ) from None
        
        finally:
            with self._aborted_transfer_obj_ids_lock:
                self._aborted_transfer_obj_ids.discard(obj_id)
            if xfer_handle:
                lingqu_agent.release_xfer_handle(xfer_handle)
            if LINGQU_REMOTE_AGENT_CACHE_MAXSIZE == 0 and remote_name:
                lingqu_agent.remove_remote_agent(remote_name)
            if added_tensor_descs:
                with self._cache_lock:
                    for tensor in tensors:
                        key = tensor.untyped_storage().data_ptr()
                        tensor_desc = self._tensor_desc_cache[key]
                        tensor_desc.metadata_count -= 1
                        if tensor_desc.metadata_count == 0:
                            lingqu_agent.deregister_memory(tensor_desc.reg_desc)
                            self._tensor_desc_cache.pop(key)
                            self._lingqu_agent_meta_version += 1
        
        return tensors

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        """发送张量（单边传输不需要实现）"""
        raise NotImplementedError(
            "灵衢传输不支持send_multiple_tensors，因为它是单边传输"
        )

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List["torch.Tensor"],
    ):
        """垃圾回收"""
        with self._cache_lock:
            assert isinstance(tensor_transport_meta, LingquTransportMetadata)
            if obj_id not in self._managed_meta_lingqu:
                return
            self._managed_meta_lingqu.pop(obj_id, None)
            for tensor in tensors:
                key = tensor.untyped_storage().data_ptr()
                if key in self._tensor_desc_cache:
                    tensor_desc = self._tensor_desc_cache[key]
                    tensor_desc.metadata_count -= 1
                    if tensor_desc.metadata_count == 0:
                        self._tensor_desc_cache.pop(key)
                        self.get_lingqu_agent().deregister_memory(tensor_desc.reg_desc)
                        self._lingqu_agent_meta_version += 1

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        """中断传输"""
        with self._aborted_transfer_obj_ids_lock:
            self._aborted_transfer_obj_ids.add(obj_id)

    def _add_tensor_descs(self, tensors: List["torch.Tensor"]):
        """添加张量描述符到缓存
        
        首次注册张量时，将底层PyTorch存储对象注册到灵衢
        """
        with self._cache_lock:
            for tensor in tensors:
                key = tensor.untyped_storage().data_ptr()
                if key in self._tensor_desc_cache:
                    self._tensor_desc_cache[key].metadata_count += 1
                else:
                    # 确定内存类型
                    # 华为NPU设备类型可能是 "npu"
                    mem_type = "npu" if tensor.device.type == "npu" else "cpu"
                    
                    # GPU ID (需根据华为设备API调整)
                    gpu_id = max(tensor.get_device(), 0)
                    
                    # ========================================
                    # TODO: 灵衢内存注册
                    # ========================================
                    try:
                        reg_desc = self.get_lingqu_agent().register_memory(
                            [
                                (
                                    tensor.untyped_storage().data_ptr(),
                                    tensor.untyped_storage().nbytes(),
                                    gpu_id,
                                    "",
                                )
                            ],
                            mem_type=mem_type,
                        )
                    except Exception as e:
                        raise RuntimeError(
                            f"灵衢内存注册失败 (size={tensor.untyped_storage().nbytes()} bytes)\n"
                            f"常见原因:\n"
                            f"  - locked memory限制过低: 检查 ulimit -l\n"
                            f"  - 灵衢内核模块未加载\n"
                            f"  - IOMMU配置问题\n"
                        ) from e
                    
                    self._tensor_desc_cache[key] = TensorDesc(reg_desc, 1)

    def _get_meta(self, object_id: str) -> Optional[LingquTransportMetadata]:
        with self._cache_lock:
            return self._managed_meta_lingqu.get(object_id)

    def _put_meta(self, object_id: str, meta: LingquTransportMetadata):
        with self._cache_lock:
            self._managed_meta_lingqu[object_id] = meta
```

### 4.2 util.py 修改

```python
# 文件: python/ray/experimental/rdt/util.py
# 修改点:

# 1. 导入新的传输类
from ray.experimental.rdt.lingqu_tensor_transport import (
    LingquTensorTransport,
)

# 2. 添加到 DEFAULT_TRANSPORTS
DEFAULT_TRANSPORTS = ["NIXL", "GLOO", "NCCL", "CUDA_IPC", "LINGQU"]

# 3. 在 _ensure_default_transports_registered() 中注册
def _ensure_default_transports_registered():
    ...
    register_tensor_transport(
        "LINGQU", ["npu", "cpu"], LingquTensorTransport, torch.Tensor
    )

# 4. 添加 register_lingqu_memory 函数
@PublicAPI(stability="alpha")
def register_lingqu_memory(tensor: "torch.Tensor") -> None:
    """注册张量内存到灵衢"""
    lingqu_transport = get_tensor_transport_manager("LINGQU")
    lingqu_transport.register_lingqu_memory(tensor)
```

### 4.3 __init__.py 修改

```python
# 文件: python/ray/experimental/rdt/__init__.py

from ray.experimental.rdt.util import (
    register_nixl_memory,
    register_lingqu_memory,  # 新增
    register_tensor_transport,
)

__all__ = [
    ...
    "register_lingqu_memory",  # 新增
]
```

```python
# 文件: python/ray/experimental/__init__.py

from ray.experimental.rdt import (
    ...
    register_lingqu_memory,  # 新增
)

__all__ = [
    ...
    "register_lingqu_memory",  # 新增
]
```

### 4.4 ray_constants.py 修改（可选）

```python
# 文件: python/ray/_private/ray_constants.py

# 添加灵衢相关常量
LINGQU_REMOTE_AGENT_CACHE_MAXSIZE = 10  # 与 NIXL 保持一致
```

## 5. 使用示例

### 5.1 基本使用

```python
import torch
import ray

@ray.remote(num_npus=1)  # 需要Ray支持NPU资源类型
class NPUActor:
    @ray.method(tensor_transport="lingqu")
    def create_tensor(self):
        return torch.randn(1000, 1000, device="npu")
    
    def process_tensor(self, tensor):
        return tensor.sum()

# 创建Actor
actors = [NPUActor.remote() for _ in range(2)]

# 创建RDT对象
ref = actors[0].create_tensor.remote()

# 传输到另一个Actor
result = actors[1].process_tensor.remote(ref)
print(ray.get(result))
```

### 5.2 ray.put / ray.get

```python
# 使用灵衢传输的 ray.put
tensor = torch.randn(100, 100, device="npu")
ref = ray.put(tensor, _tensor_transport="lingqu")

# ray.get 自动使用灵衢传输
result = ray.get(ref)
```

### 5.3 预注册内存

```python
from ray.experimental import register_lingqu_memory

@ray.remote(num_npus=1)
class Trainer:
    def __init__(self):
        self.weight = torch.randn(1000, 1000, device="npu")
        # 预注册内存，提升多次传输性能
        register_lingqu_memory(self.weight)
    
    @ray.method(tensor_transport="lingqu")
    def get_weight(self):
        return self.weight
```

## 6. 实现步骤

### Phase 1: SDK调研与接口对接（预估2周）

1. **调研灵衢SDK API**
   - 内存注册接口
   - 传输初始化接口
   - Agent管理接口
   - RDMA传输接口
   
2. **确定关键参数**
   - 设备类型标识（"npu" 或其他）
   - 内存类型标识
   - GPU/NPU ID获取方式
   - 同步API

### Phase 2: 代码实现（预估1周）

1. 创建 `lingqu_tensor_transport.py`
2. 修改 `util.py`、`__init__.py`
3. 添加常量定义
4. 编写单元测试

### Phase 3: 集成测试（预估1周）

1. 单节点测试
2. 多节点RDMA测试
3. 性能基准测试
4. 错误处理测试

### Phase 4: 文档与发布

1. 更新官方文档
2. 编写使用指南
3. 性能优化建议

## 7. 关键技术点

### 7.1 需要确认的灵衢API

| 功能 | NIXL API | 灵衢对应API（需确认） |
|------|----------|---------------------|
| Agent创建 | `nixl_agent(name, config)` | `lingqu_agent(...)` |
| 内存注册 | `agent.register_memory(...)` | `agent.register_memory(...)` |
| 描述符序列化 | `agent.get_serialized_descs(...)` | `...` |
| 添加远程Agent | `agent.add_remote_agent(meta)` | `...` |
| 初始化传输 | `agent.initialize_xfer(...)` | `...` |
| 执行传输 | `agent.transfer(handle)` | `...` |
| 检查状态 | `agent.check_xfer_state(handle)` | `...` |
| 取消注册 | `agent.deregister_memory(desc)` | `...` |

### 7.2 设备类型适配

华为设备可能使用以下类型：
- `"npu"` - 华达NPU
- `"cuda"` - 如果使用华为GPU（昇腾兼容模式）

需要在以下位置适配：
```python
# 灵衢传输中
if device.type == "npu":
    torch.npu.synchronize(dev)  # 或 torch.cuda.synchronize

# 内存类型
mem_type = "npu" if tensor.device.type == "npu" else "cpu"
```

### 7.3 Ray资源类型扩展

如果Ray尚未支持NPU资源类型，需要扩展：
```python
# 在 ray.remote decorator中支持
@ray.remote(num_npus=1)

# 或使用动态资源
@ray.remote(resources={"NPU": 1})
```

## 8. 测试策略

### 8.1 单元测试文件

```
python/ray/tests/rdt/
├── test_rdt_lingqu.py          # [新增] 灵衢传输测试
```

### 8.2 测试用例

```python
# test_rdt_lingqu.py

import pytest
import torch
import ray
from ray.experimental import register_lingqu_memory

def test_lingqu_basic_transfer():
    """基本传输测试"""
    @ray.remote(num_npus=1)
    class Actor:
        @ray.method(tensor_transport="lingqu")
        def create(self):
            return torch.randn(100, 100, device="npu")
        
        def sum(self, t):
            return t.sum().item()
    
    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].create.remote()
    result = actors[1].sum.remote(ref)
    assert ray.get(result) != 0

def test_lingqu_multi_tensor():
    """多张量传输测试"""
    @ray.remote(num_npus=1)
    class Actor:
        @ray.method(tensor_transport="lingqu")
        def create_multi(self):
            return [
                torch.randn(50, 50, device="npu"),
                torch.randn(100, 100, device="npu"),
            ]
        
        def process(self, tensors):
            return sum(t.sum().item() for t in tensors)
    
    ...

def test_lingqu_memory_registration():
    """内存预注册测试"""
    @ray.remote(num_npus=1)
    class Actor:
        def __init__(self):
            self.tensor = torch.randn(1000, 1000, device="npu")
            register_lingqu_memory(self.tensor)
        
        @ray.method(tensor_transport="lingqu")
        def get_tensor_view(self, idx):
            return self.tensor[idx]
    
    ...

def test_lingqu_error_handling():
    """错误处理测试"""
    # 测试传输中断
    # 测试Actor故障处理
    # 测试超时处理
    ...
```

## 9. 性能优化建议

### 9.1 内存预注册

```python
# 预注册常驻内存，避免重复注册开销
register_lingqu_memory(weight_tensor)
```

### 9.2 Agent缓存

```python
# 在 ray_constants.py 中调整
LINGQU_REMOTE_AGENT_CACHE_MAXSIZE = 20  # 根据实际需求调整
```

### 9.3 批量传输

```python
@ray.method(tensor_transport="lingqu")
def get_batch(self):
    # 返回张量列表，一次传输多个
    return [tensor1, tensor2, tensor3]
```

## 10. 风险与限制

### 10.1 已知限制

1. **设备支持**: 仅支持华为NPU/CPU
2. **张量类型**: 目前仅支持 `torch.Tensor`
3. **Actor限制**: 仅支持Ray Actor任务
4. **异步支持**: 尚未支持 asyncio

### 10.2 需要解决的问题

1. Ray对NPU资源类型的支持
2. torch.npu API的兼容性
3. 灵衢SDK的Python绑定
4. 跨节点RDMA配置

## 11. 参考资料

### 11.1 现有实现参考

- `nixl_tensor_transport.py` - NIXL完整实现
- `cuda_ipc_transport.py` - CUDA IPC实现
- `collective_tensor_transport.py` - NCCL/Gloo实现
- `test_rdt_custom.py` - 自定义传输测试示例

### 11.2 文档参考

- `doc/source/ray-core/direct-transport.rst` - RDT官方文档
- NVIDIA GPUDirect RDMA 文档
- 华为灵衢总线技术文档（需补充）

---

## 附录A: 文件修改清单汇总

| 序号 | 文件路径 | 操作 | 说明 |
|------|---------|------|------|
| 1 | `python/ray/experimental/rdt/lingqu_tensor_transport.py` | 新增 | 灵衢传输核心实现 |
| 2 | `python/ray/experimental/rdt/util.py` | 修改 | 注册LINGQU到默认传输 |
| 3 | `python/ray/experimental/rdt/__init__.py` | 修改 | 导出register_lingqu_memory |
| 4 | `python/ray/experimental/__init__.py` | 修改 | 导出新接口 |
| 5 | `python/ray/_private/ray_constants.py` | 修改 | 添加灵衢常量 |
| 6 | `doc/source/ray-core/direct-transport.rst` | 修改 | 添加灵衢使用文档 |
| 7 | `python/ray/tests/rdt/test_rdt_lingqu.py` | 新增 | 灵衢传输测试 |

---

## 附录B: 灵衢SDK API对接清单

**需要华为提供的关键API信息**:

1. **Agent初始化**
   ```python
   # 类似NIXL的Agent创建方式
   agent = lingqu_agent(name, config)
   ```

2. **内存注册**
   ```python
   # 注册GPU/NPU内存区域
   desc = agent.register_memory(
       [(data_ptr, size, device_id, flags)],
       mem_type="npu"
   )
   ```

3. **描述符管理**
   ```python
   xfer_descs = agent.get_xfer_descs(tensors)
   serialized = agent.get_serialized_descs(xfer_descs)
   descs = agent.deserialize_descs(serialized)
   ```

4. **Agent通信**
   ```python
   agent_meta = agent.get_agent_metadata()
   agent.add_remote_agent(remote_meta)
   agent.remove_remote_agent(name)
   ```

5. **传输操作**
   ```python
   handle = agent.initialize_xfer("READ", local_descs, remote_descs, remote_name, uuid)
   state = agent.transfer(handle)
   state = agent.check_xfer_state(handle)
   agent.release_xfer_handle(handle)
   ```

6. **内存释放**
   ```python
   agent.deregister_memory(desc)
   ```

---

*文档版本: v1.0*
*创建日期: 2026-05-18*
*适用Ray版本: 2.55.1*