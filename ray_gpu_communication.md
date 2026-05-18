# Ray架构中的GPU通信支持

基于Ray源码分析，Ray主要通过以下三种机制支持GPU间通信：

## 1. NCCL集体通信层

**核心实现**：`python/ray/util/collective/collective_group/nccl_collective_group.py:121-836`

### NCCLGroup类架构

```
NCCLGroup (world_size, rank, group_name)
├── _dev_comm_map: NCCL communicator缓存
├── _dev_streams_map: CUDA stream缓存
├── _dev_event_map: CUDA event缓存
└── _used_gpu_indices: 已使用GPU索引集合
```

### 主要通信操作

| 操作 | 方法 | 说明 |
|------|------|------|
| AllReduce | `allreduce()` | 跨组归约所有tensor |
| Broadcast | `broadcast()` | 从源rank广播到所有rank |
| AllGather | `allgather()` | 收集所有rank的tensor |
| Reduce | `reduce()` | 归约到目标rank |
| Send/Recv | `send()/recv()` | P2P点对点通信 |
| Barrier | `barrier()` | 同步所有进程 |

### Rendezvous机制

**实现位置**：`nccl_collective_group.py:29-119`

```
进程A (rank=0)                      进程B (rank=1)
    │                                   │
    ├─ 生成 NCCLUniqueID                │
    ├─ 存入 NCCLUniqueIDStore           │
    │      (Ray Named Actor)            │
    │                                   ├─ Rendezvous.meet()
    │                                   ├─ 获取 NCCLUniqueID
    │                                   │
    └─ 创建 Communicator ←─────────────┘
```

### 关键代码片段

```python
# nccl_collective_group.py:395-449
def _get_nccl_collective_communicator(self, comm_key, device_list):
    # 缓存查找
    if comm_key in self._dev_comm_map:
        return self._dev_comm_map[comm_key]
    
    # Rank 0生成UID，其他rank获取UID
    if self.rank == 0:
        nccl_uid = self._generate_nccl_uid(group_key)
    else:
        rendezvous = Rendezvous(group_key)
        rendezvous.meet()
        nccl_uid = rendezvous.get_nccl_id()
    
    # 创建NCCL communicator
    nccl_util.groupStart()
    for i, device in enumerate(device_list):
        actual_rank = self.rank * len(device_list) + i
        comms[i] = nccl_util.create_nccl_communicator(
            actual_world_size, nccl_uid, actual_rank
        )
    nccl_util.groupEnd()
```

---

## 2. Compiled Graph Tensor Transport

**核心实现**：`python/ray/dag/compiled_dag_node.py` + `src/ray/core_worker/common.h`

### API接口

```python
# dag_node.py:141-361
def with_tensor_transport(
    self,
    transport: str = "nccl",  # "nccl" | "shm" | "auto"
    device: str = "gpu",
    _static_shape: bool = False,
    _direct_return: bool = False
)
```

### 使用示例

```python
from ray.dag import DAGNode

# GPU-to-GPU通信
dag = receiver.method.bind(
    sender.method.bind(inp).with_tensor_transport(transport="nccl")
)

# 编译并执行
executable_dag = dag.experimental_compile()
result = executable_dag.execute(input_tensor)
```

### C++层实现

```cpp
// src/ray/core_worker/common.h:115-116
struct TaskOptions {
    std::optional<std::string> tensor_transport;  // NCCL/GLOO等
    bool enable_tensor_transport = false;
};

// src/ray/core_worker/reference_counter.h:318-323
struct ObjectReference {
    std::optional<std::string> tensor_transport_;
    // 记录tensor传输方式
};
```

### 通信流程

```
┌─────────────┐       NCCL P2P        ┌─────────────┐
│  Actor A    │───────────────────────│  Actor B    │
│  (GPU 0)    │                       │  (GPU 1)    │
│             │                       │             │
│  TaskSpec   │                       │  TaskSpec   │
│  tensor_    │                       │  tensor_    │
│  transport  │                       │  transport  │
└─────────────┘                       └─────────────┘
      │                                     │
      └─────────┬───────────────────────────┘
                │
         ┌──────▼──────┐
         │ NCCL Comm   │
         │ (Direct)    │
         │ (绕过Object  │
         │  Store)     │
         └─────────────┘
```

---

## 3. GPU通信与计算重叠（实验性）

**实现位置**：`compiled_dag_node.py:190-335`

### 启用方式

```python
dag.experimental_compile(
    _overlap_gpu_communication=True  # 实验性特性
)
```

### 工作原理

```python
# compiled_dag_node.py:657-677
def _read(self, overlap_gpu_communication: bool) -> bool:
    # 启用overlap时，将读取操作包装为GPUFuture
    # 通信与计算可在不同CUDA stream并行执行
    if overlap_gpu_communication:
        return wrap_in_gpu_future=True
```

---

## 依赖库关系

```
ray.util.collective
├── cupy.cuda.nccl
│   ├── NcclCommunicator        # NCCL communicator封装
│   ├── groupStart/groupEnd     # NCCL组操作
│   └── get_unique_id()         # 生成NCCL UID
│
├── torch.Tensor                # PyTorch tensor支持
│   └── data_ptr()              # 获取GPU内存指针
│   └── record_stream()         # 记录CUDA stream
│
└── numpy.ndarray               # CPU tensor支持
```

**关键映射**：`nccl_util.py:22-87`

```python
NCCL_REDUCE_OP_MAP = {
    ReduceOp.SUM: nccl.NCCL_SUM,
    ReduceOp.PRODUCT: nccl.NCCL_PROD,
    ReduceOp.MIN: nccl.NCCL_MIN,
    ReduceOp.MAX: nccl.NCCL_MAX,
}

TORCH_NCCL_DTYPE_MAP = {
    torch.float32: nccl.NCCL_FLOAT,
    torch.float16: nccl.NCCL_FLOAT16,
    torch.int64: nccl.NCCL_INT64,
    # ... 更多类型映射
}
```

---

## 关键源码位置索引

| 功能模块 | 文件路径 | 关键代码行 |
|---------|---------|-----------|
| NCCL Group实现 | `nccl_collective_group.py` | L121-836 |
| NCCL API封装 | `nccl_util.py` | L1-297 |
| Rendezvous机制 | `nccl_collective_group.py` | L29-119 |
| Tensor Transport API | `dag_node.py` | L141-361 |
| Compiled DAG执行 | `compiled_dag_node.py` | L185-335 |
| C++ Task选项 | `core_worker/common.h` | L77-116, L137-157 |
| Object引用追踪 | `reference_counter.h` | L318-323 |

---

## 总结

Ray的GPU通信架构通过三层实现：

1. **底层**：NCCL/cupy提供硬件级GPU通信能力
2. **中间层**：`ray.util.collective`封装集体通信API
3. **高层**：Compiled Graph提供声明式DAG + tensor transport

**优势**：
- 绕过Ray Object Store，降低CPU-GPU拷贝开销
- 支持多GPU、多进程、多节点通信
- 自动管理communicator生命周期
- 实验性支持通信/计算重叠

**限制**：
- NCCL需要cupy依赖
- P2P通信不支持同进程内的GPU间传输
- Compiled Graph tensor transport目前仅支持P2P（集体通信开发中）