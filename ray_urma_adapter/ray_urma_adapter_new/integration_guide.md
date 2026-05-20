# Ray RDT UMDK适配集成指南

## 一、概述

本指南说明如何将华为灵衢UMDK URMA适配层集成到Ray RDT中，使Ray能够在华为灵衢总线超节点集群上实现GPU/NPU数据直通能力。

## 二、前置条件

### 2.1 系统要求

| 要求 | 说明 |
|-----|------|
| 操作系统 | Linux (华为EulerOS 22.03+) 或 Windows |
| Ray版本 | Ray 2.55.1+ |
| UMDK版本 | UMDK 1.0+ |
| Python | Python 3.8+ |
| PyTorch | PyTorch 2.0+ (torch_npu支持) |
| 硬件 | 华为Ascend NPU (910B/310P等) |

### 2.2 软件依赖

```
# Python依赖
torch>=2.0.0
torch-npu>=2.0.0  # 华达Ascend PyTorch扩展
pyurma>=1.0.0     # UMDK Python绑定 (需自行构建)

# 系统依赖
liburma.so        # UMDK URMA库
liburpc.so        # UMDK URPC库
libcam.so         # CAM算子库
```

## 三、文件结构

### 3.1 Ray源码修改位置

```
ray-ray-2.55.1/
└── python/
    └── ray/
        └── experimental/
            └── rdt/
                ├── util.py                        # 修改: 注册URMA后端
                ├── urma_tensor_transport.py      # 新增: URMA传输实现
                ├── npu_ipc_transport.py          # 新增: NPU IPC传输
                ├── cam_collective_transport.py   # 新增: CAM集合通信
                ├── rdt_manager.py                # 修改: 添加URMA abort
                └── rdt_store.py                  # 修改: 添加NPU支持
```

### 3.2 新增适配层文件

```
ray_urma_adapter/
├── urma_tensor_transport.py   # URMA传输实现
├── npu_ipc_transport.py       # NPU IPC实现
├── cam_collective_transport.py # CAM集合通信
├── urma_types.py              # URMA类型定义
└── __init__.py                # 模块入口
```

### 3.3 pyurma Python绑定

```
pyurma/
├── __init__.py
├── core.py                    # 核心URMA API绑定
├── types.py                   # 数据类型定义
├── agent.py                   # UrmaAgent高级封装
├── errors.py                  # 错误定义
├── utils.py                   # 工具函数
└── _pyurma.so                 # C扩展模块
```

## 四、集成步骤

### 步骤1: 构建pyurma Python绑定

#### 1.1 下载UMDK源码

```bash
# UMDK源码位置
D:\C++\umdk-master\umdk-master\

# 关键头文件
src/urma/lib/urma/core/include/urma_api.h
src/urma/lib/urma/core/include/urma_types.h
src/urpc/include/framework/urpc_framework_api.h
```

#### 1.2 创建pybind11绑定

```bash
# 创建绑定目录
mkdir -p pyurma/bindings

# 编写绑定代码 (参考pyurma_design.md)
# pyurma/bindings/pyurma_bindings.cpp
```

#### 1.3 编译

```bash
# Linux
cd pyurma
python setup.py build_ext --inplace
python setup.py install

# Windows (Visual Studio)
cmake -B build -S .
cmake --build build --config Release
```

#### 1.4 验证

```python
# test_pyurma.py
from pyurma import UrmaAgent

agent = UrmaAgent()
agent.initialize()
print("pyurma initialized successfully")
agent.cleanup()
```

### 步骤2: 修改Ray RDT源码

#### 2.1 修改util.py注册URMA后端

**文件**: `python/ray/experimental/rdt/util.py`

```python
# 在文件开头添加导入
from ray.experimental.rdt.urma_tensor_transport import UrmaTensorTransport
from ray.experimental.rdt.npu_ipc_transport import NpuIpcTransport
from ray.experimental.rdt.cam_collective_transport import CamCollectiveTransport

# 修改 _ensure_default_transports_registered() 函数
def _ensure_default_transports_registered():
    """Register all default tensor transports."""
    global _default_transports_registered
    if _default_transports_registered:
        return
    
    # 注册NVIDIA传输后端
    if _check_nixl_available():
        register_tensor_transport(
            "NIXL",
            ["cuda", "cpu"],
            NixlTensorTransport,
            torch.Tensor
        )
    
    # 注册URMA传输后端 (新增)
    if _check_urma_available():
        register_tensor_transport(
            "URMA",
            ["npu", "cpu"],
            UrmaTensorTransport,
            torch.Tensor
        )
    
    # 注册NPU IPC传输后端 (新增)
    if _check_torch_npu_available():
        register_tensor_transport(
            "NPU_IPC",
            ["npu"],
            NpuIpcTransport,
            torch.Tensor
        )
    
    # 注册CAM集合通信后端 (新增)
    if _check_cam_available():
        register_tensor_transport(
            "CAM",
            ["npu"],
            CamCollectiveTransport,
            torch.Tensor
        )
    
    # 其他后端...
    _default_transports_registered = True

# 添加检测函数
def _check_urma_available() -> bool:
    """检查URMA是否可用"""
    try:
        from pyurma import UrmaAgent
        return True
    except ImportError:
        return False

def _check_torch_npu_available() -> bool:
    """检查torch_npu是否可用"""
    try:
        import torch_npu
        return torch.npu.is_available()
    except ImportError:
        return False

def _check_cam_available() -> bool:
    """检查CAM算子是否可用"""
    try:
        from cam import SyncCollectives
        return True
    except ImportError:
        return False

# 修改DEFAULT_TRANSPORTS顺序
DEFAULT_TRANSPORTS = ["URMA", "NPU_IPC", "CAM", "NIXL", "NCCL", "GLOO", "CUDA_IPC"]
```

#### 2.2 复制urma_tensor_transport.py到Ray目录

```bash
# 将适配文件复制到Ray RDT目录
cp ray_urma_adapter/urma_tensor_transport.py \
   D:\C++\ray-ray-2.55.1\ray-ray-2.55.1\python\ray\experimental\rdt\
```

#### 2.3 修改rdt_manager.py添加URMA abort

**文件**: `python/ray/experimental/rdt/rdt_manager.py`

```python
# 在 abort_tensor_transport 方法中添加URMA处理
def abort_tensor_transport(self, obj_id: str) -> None:
    """Abort any ongoing tensor transport for the given object."""
    
    # 获取传输后端
    transport = self._get_tensor_transport(obj_id)
    
    if transport is None:
        return
    
    backend = transport.tensor_transport_backend()
    
    # 添加URMA/NPU_IPC/CAM的处理
    if backend in ["URMA", "NPU_IPC", "CAM", "NIXL", "CUDA_IPC"]:
        transport.abort_tensor_transport(obj_id)
    
    # 其他处理...
```

#### 2.4 修改rdt_store.py添加NPU支持

**文件**: `python/ray/experimental/rdt/rdt_store.py`

```python
# 在 RDTStoreMetadata 类中添加NPU设备支持
class RDTStoreMetadata:
    """Metadata for RDT store."""
    
    def __init__(self, ...):
        # 添加NPU设备检测
        self._supported_devices = ["cuda", "npu", "cpu"]
        
    def _detect_device(self, tensors: List[torch.Tensor]) -> str:
        """检测张量设备类型"""
        for tensor in tensors:
            device_str = str(tensor.device)
            if 'npu' in device_str:
                return 'npu'
            elif 'cuda' in device_str:
                return 'cuda'
        return 'cpu'
```

### 步骤3: 修改Ray Worker初始化

**文件**: `python/ray/_private/worker.py`

```python
# 在 connect() 函数中添加URMA初始化
def connect(...):
    """Connect to Ray cluster."""
    
    # 原有初始化...
    
    # 添加URMA初始化 (新增)
    if _check_urma_available():
        from pyurma import urma_init
        urma_init({
            'log_level': 'info',
            'max_contexts': 16,
            'max_jettys': 64
        })
        logger.info("URMA initialized for Ray worker")
```

### 步骤4: 添加torch_npu支持

#### 4.1 安装torch_npu

```bash
# 安装华为Ascend PyTorch扩展
pip install torch-npu

# 或从源码编译
git clone https://gitee.com/ascend/pytorch.git
cd pytorch
python setup.py install
```

#### 4.2 验证NPU可用

```python
import torch
import torch_npu

print(f"NPU available: {torch.npu.is_available()}")
print(f"NPU count: {torch.npu.device_count()}")

# 创建NPU张量
x = torch.randn(100, 100, device="npu:0")
print(f"Tensor device: {x.device}")
```

## 五、使用示例

### 5.1 URMA传输示例

```python
import ray
import torch
import torch_npu

# 初始化Ray (指定使用URMA)
ray.init(
    tensor_transport_backend="URMA"
)

# 创建Actor
@ray.remote(num_gpus=1)
class NPUSender:
    def __init__(self):
        self.tensor = torch.randn(1024, 1024, device="npu:0")
    
    def get_tensor(self):
        # 返回张量时使用URMA传输
        return self.tensor

@ray.remote(num_gpus=1)
class NPUReceiver:
    def receive_tensor(self, tensor):
        # 接收张量 (自动使用URMA)
        result = tensor @ tensor.T
        return result

# 启动Actor
sender = NPUSender.remote()
receiver = NPUReceiver.remote()

# 获取张量 (触发URMA传输)
tensor_ref = sender.get_tensor.remote()

# 接收并处理
result_ref = receiver.receive_tensor.remote(tensor_ref)
result = ray.get(result_ref)

print(f"Result shape: {result.shape}")
print(f"Result device: {result.device}")

ray.shutdown()
```

### 5.2 CAM集合通信示例

```python
import ray
import torch
import torch_npu

# 初始化Ray
ray.init()

# 创建分布式Actor组
@ray.remote(num_gpus=1)
class NPUDistributed:
    def __init__(self, rank, world_size):
        self.rank = rank
        self.world_size = world_size
    
    def setup_collective_group(self, ranks):
        # 设置CAM通信组
        ray.experimental.collective.init_collective_group(
            ranks=ranks,
            backend="CAM"  # 使用CAM后端
        )
    
    def all_reduce(self, tensor):
        # CAM AllReduce
        result = ray.experimental.collective.all_reduce(
            tensor,
            op="sum",
            group_name="default"
        )
        return result

# 创建Actor组
num_workers = 4
actors = [
    NPUDistributed.remote(i, num_workers)
    for i in range(num_workers)
]

# 设置通信组
ranks = list(range(num_workers))
ray.get([a.setup_collective_group.remote(ranks) for a in actors])

# 执行AllReduce
input_tensor = torch.randn(100, 100, device="npu:0")
results = ray.get([a.all_reduce.remote(input_tensor) for a in actors])

ray.shutdown()
```

### 5.3 NPU IPC同节点传输示例

```python
import ray
import torch
import torch_npu

ray.init()

# 同节点Actor (使用NPU IPC)
@ray.remote(num_gpus=1, placement_group=None)
class NPULocalActor:
    def __init__(self):
        self.tensor = torch.randn(1024, 1024, device="npu:0")
    
    def send_tensor(self):
        return self.tensor
    
    def recv_tensor(self, tensor):
        return tensor @ tensor.T

# 创建同节点Actor
actor1 = NPULocalActor.remote()
actor2 = NPULocalActor.remote()

# NPU IPC传输
tensor_ref = actor1.send_tensor.remote()
result = ray.get(actor2.recv_tensor.remote(tensor_ref))

ray.shutdown()
```

## 六、性能优化建议

### 6.1 URMA传输优化

```python
# 批量注册内存
agent.register_memory([tensor1, tensor2, tensor3])  # 批量更高效

# 使用适当的传输大小
# 小数据 (< 1KB): 使用SEND/RECV模式
# 中等数据 (1KB - 1MB): 使用WRITE模式
# 大数据 (> 1MB): 使用READ模式

# 调整完成队列大小
urma_create_jetty(ctx, max_jfs=256, max_jfr=256, jfc_size=4096)

# 使用inline传输 (小数据)
urma_write(jfs, ..., flags=URMA_INLINE_FLAG)
```

### 6.2 内存注册缓存

```python
# 在UrmaAgent中缓存已注册的内存段
class UrmaAgent:
    def __init__(self):
        self._seg_cache = {}  # 缓存已注册段
    
    def register_memory(self, tensors):
        # 检查是否已注册
        for tensor in tensors:
            key = (tensor.data_ptr(), tensor.nbytes())
            if key in self._seg_cache:
                return self._seg_cache[key]
        
        # 新注册
        handles = self._do_register(tensors)
        for tensor, handle in zip(tensors, handles):
            key = (tensor.data_ptr(), tensor.nbytes())
            self._seg_cache[key] = handle
        
        return handles
```

### 6.3 传输路径优化

```python
# 使用urma_advise_jetty预建立传输路径
urma_advise_jetty(
    ctx,
    jetty,
    remote_jetty,
    URMA_ADVICE_CREATE_TP,  # 创建传输路径
    timeout=5000  # 5秒超时
)

# 批量传输
for local_seg, remote_seg in zip(local_segs, remote_segs):
    urma_read(jfs, remote_jetty, local_seg, remote_seg, ...)

urma_wait_jfc(jfc, timeout_ms=30000)  # 批量等待
```

## 七、调试与问题排查

### 7.1 URMA调试日志

```python
# 启用URMA调试日志
import logging
logging.getLogger("pyurma").setLevel(logging.DEBUG)

# 或在初始化时配置
urma_init({'log_level': 'debug'})
```

### 7.2 常见问题

| 问题 | 原因 | 解决方案 |
|-----|------|---------|
| URMA初始化失败 | liburma.so未找到 | 安装UMDK并设置LD_LIBRARY_PATH |
| NPU内存注册失败 | torch_npu未正确安装 | 重新安装torch_npu |
| 传输超时 | 网络连接问题或防火墙 | 检查网络配置和端口开放 |
| Token验证失败 | Token策略不匹配 | 确认两端使用相同的Token策略 |
| DSVA失败 | NPU驱动不支持 | 更新NPU驱动版本 |

### 7.3 诊断脚本

```python
# diagnose_urma.py
import torch
import torch_npu

def check_urma():
    print("=== URMA Diagnostics ===")
    
    # 检查torch_npu
    print(f"torch_npu available: {torch.npu.is_available()}")
    print(f"NPU count: {torch.npu.device_count()}")
    
    for i in range(torch.npu.device_count()):
        print(f"NPU {i}: {torch.npu.get_device_name(i)}")
    
    # 检查pyurma
    try:
        from pyurma import UrmaAgent
        print("pyurma: OK")
        
        agent = UrmaAgent()
        agent.initialize()
        print("URMA init: OK")
        
        # 测试内存注册
        tensor = torch.randn(100, 100, device="npu:0")
        handles = agent.register_memory([tensor])
        print(f"Memory registration: OK ({len(handles)} handles)")
        
        agent.cleanup()
        print("URMA cleanup: OK")
        
    except Exception as e:
        print(f"URMA: FAILED - {e}")
    
    print("=== Diagnostics Complete ===")

check_urma()
```

## 八、测试验证

### 8.1 单元测试

```python
# tests/test_urma_transport.py
import pytest
import ray
import torch
import torch_npu

def test_urma_init():
    """测试URMA初始化"""
    ray.init(tensor_transport_backend="URMA")
    
    @ray.remote(num_gpus=1)
    class TestActor:
        def test(self):
            return torch.randn(100, 100, device="npu:0")
    
    actor = TestActor.remote()
    result = ray.get(actor.test.remote())
    
    assert result.device.type == "npu"
    
    ray.shutdown()

def test_urma_transfer():
    """测试URMA数据传输"""
    ray.init(tensor_transport_backend="URMA")
    
    @ray.remote(num_gpus=1)
    class Sender:
        def get_tensor(self):
            return torch.ones(1000, 1000, device="npu:0")
    
    @ray.remote(num_gpus=1)
    class Receiver:
        def check_tensor(self, tensor):
            assert tensor.shape == (1000, 1000)
            assert tensor.sum().item() == 1000 * 1000
            return True
    
    sender = Sender.remote()
    receiver = Receiver.remote()
    
    tensor_ref = sender.get_tensor.remote()
    result = ray.get(receiver.check_tensor.remote(tensor_ref))
    
    assert result == True
    
    ray.shutdown()

def test_urma_large_transfer():
    """测试大数据传输"""
    ray.init(tensor_transport_backend="URMA")
    
    @ray.remote(num_gpus=1)
    class LargeSender:
        def get_large_tensor(self):
            return torch.randn(10240, 10240, device="npu:0")  # 400MB
    
    @ray.remote(num_gpus=1)
    class LargeReceiver:
        def receive_large(self, tensor):
            return tensor.shape
    
    sender = LargeSender.remote()
    receiver = LargeReceiver.remote()
    
    tensor_ref = sender.get_large_tensor.remote()
    shape = ray.get(receiver.receive_large.remote(tensor_ref))
    
    assert shape == (10240, 10240)
    
    ray.shutdown()
```

### 8.2 性能测试

```python
# tests/test_urma_performance.py
import time
import ray
import torch
import torch_npu

def benchmark_urma_transfer(size_mb=100, iterations=10):
    """测试URMA传输性能"""
    ray.init(tensor_transport_backend="URMA")
    
    @ray.remote(num_gpus=1)
    class Sender:
        def get_tensor(self, size_mb):
            elements = size_mb * 1024 * 1024 // 4  # float32
            return torch.randn(elements, device="npu:0")
    
    @ray.remote(num_gpus=1)
    class Receiver:
        def receive(self, tensor):
            return tensor.sum().item()
    
    sender = Sender.remote()
    receiver = Receiver.remote()
    
    times = []
    
    for i in range(iterations):
        start = time.time()
        
        tensor_ref = sender.get_tensor.remote(size_mb)
        ray.get(receiver.receive.remote(tensor_ref))
        
        end = time.time()
        times.append(end - start)
    
    avg_time = sum(times) / len(times)
    bandwidth = size_mb / avg_time  # MB/s
    
    print(f"Size: {size_mb}MB")
    print(f"Average time: {avg_time:.3f}s")
    print(f"Bandwidth: {bandwidth:.2f} MB/s")
    
    ray.shutdown()
    
    return bandwidth

# 运行测试
benchmark_urma_transfer(size_mb=10)
benchmark_urma_transfer(size_mb=100)
benchmark_urma_transfer(size_mb=1000)
```

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
export LD_LIBRARY_PATH=/usr/lib/umdk:$LD_LIBRARY_PATH
export URMA_LOG_LEVEL=info
export RAY_TENSOR_TRANSPORT_BACKEND=URMA
```

### 9.2 Ray集群配置

```yaml
# ray_cluster_config.yaml
cluster_name: npu_cluster

provider:
  type: external
  head_ip: 10.0.0.1
  worker_ips: [10.0.0.2, 10.0.0.3, 10.0.0.4]

available_node_types:
  npu_head:
    node_config:
      resources:
        NPU: 8
        CPU: 64
        memory: 256G
  
  npu_worker:
    node_config:
      resources:
        NPU: 8
        CPU: 64
        memory: 256G

docker:
  image: ray-npu:latest
  run_options:
    - "--privileged"
    - "--device=/dev/davinci0"
    - "--device=/dev/davinci1"
    - "-v/usr/lib/umdk:/usr/lib/umdk"

runtime_env:
  env_vars:
    RAY_TENSOR_TRANSPORT_BACKEND: URMA
    URMA_LOG_LEVEL: info
```

---

*文档版本: 1.0*
*最后更新: 2025年*
*作者: AI分析助手*