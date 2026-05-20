# pyurma - UMDK URMA Python绑定设计文档

## 一、概述

pyurma是UMDK URMA API的Python绑定，提供与NIXL类似的易用接口，用于Ray RDT适配。

## 二、模块结构

```
pyurma/
├── __init__.py           # 主入口，导出公共API
├── core.py               # 核心URMA API绑定
├── types.py              # 数据类型定义
├── agent.py              # UrmaAgent高级封装
├── errors.py             # 错误定义
├── utils.py              # 工具函数
└── _pyurma.so            # C扩展模块
```

## 三、API设计

### 3.1 核心API (core.py)

```python
"""
URMA Core API - 直接映射UMDK C API
"""

from typing import List, Optional, Tuple
from ctypes import c_void_p, c_uint64, c_int32, c_uint8, Structure, POINTER

# ============ 初始化与清理 ============

def urma_init(config: Optional[dict] = None) -> bool:
    """
    初始化URMA库
    对应UMDK: urma_init()
    
    Args:
        config: 配置字典，包含:
            - log_level: 日志级别
            - max_contexts: 最大上下文数
            - max_jettys: 最大jetty数
    
    Returns:
        初始化是否成功
    """
    pass

def urma_uninit() -> None:
    """
    清理URMA库
    对应UMDK: urma_uninit()
    """
    pass

# ============ 设备管理 ============

def urma_get_device_list() -> List['UrmaDevice']:
    """
    获取可用设备列表
    对应UMDK: urma_get_device_list()
    
    Returns:
        设备列表，每个设备包含:
            - name: 设备名称
            - guid: 设备GUID
            - transport_type: 传输类型
    """
    pass

def urma_get_device_attr(device: 'UrmaDevice') -> 'UrmaDeviceAttr':
    """
    获取设备属性
    对应UMDK: urma_get_device_attr()
    """
    pass

# ============ 上下文管理 ============

def urma_create_context(device: 'UrmaDevice', flags: int = 0) -> 'UrmaContext':
    """
    创建URMA上下文
    对应UMDK: urma_create_context()
    
    Args:
        device: 设备对象
        flags: 创建标志
    
    Returns:
        URMA上下文对象
    """
    pass

def urma_delete_context(ctx: 'UrmaContext') -> None:
    """
    删除URMA上下文
    对应UMDK: urma_delete_context()
    """
    pass

# ============ Jetty管理 (通信端点) ============

def urma_create_jetty(
    ctx: 'UrmaContext',
    transport_mode: int = URMA_TM_RC,
    max_jfs: int = 128,
    max_jfr: int = 128,
    jfc_size: int = 1024
) -> 'UrmaJetty':
    """
    创建双向Jetty
    对应UMDK: urma_create_jetty()
    
    Args:
        ctx: URMA上下文
        transport_mode: 传输模式 (URMA_TM_RM/RC/UM)
        max_jfs: 最大发送队列深度
        max_jfr: 最大接收队列深度
        jfc_size: 完成队列大小
    
    Returns:
        Jetty对象
    """
    pass

def urma_create_jfs(ctx: 'UrmaContext', max_wr: int = 128) -> 'UrmaJfs':
    """
    创建发送端Jetty
    对应UMDK: urma_create_jfs()
    """
    pass

def urma_create_jfr(ctx: 'UrmaContext', max_wr: int = 128) -> 'UrmaJfr':
    """
    创建接收端Jetty
    对应UMDK: urma_create_jfr()
    """
    pass

def urma_delete_jetty(jetty: 'UrmaJetty') -> None:
    """
    删除Jetty
    对应UMDK: urma_delete_jetty()
    """
    pass

def urma_get_jetty_id(jetty: 'UrmaJetty') -> 'UrmaJettyId':
    """
    获取Jetty ID (用于导入到远程)
    对应UMDK: urma_get_jetty_id()
    """
    pass

def urma_import_jetty(ctx: 'UrmaContext', jetty_id: 'UrmaJettyId') -> 'UrmaRemoteJetty':
    """
    导入远程Jetty
    对应UMDK: urma_import_jetty()
    """
    pass

def urma_unimport_jetty(ctx: 'UrmaContext', remote_jetty: 'UrmaRemoteJetty') -> None:
    """
    取消导入Jetty
    对应UMDK: urma_unimport_jetty()
    """
    pass

# ============ 内存管理 ============

def urma_register_seg(ctx: 'UrmaContext', seg_cfg: 'UrmaSegCfg') -> 'UrmaSeg':
    """
    注册内存段
    对应UMDK: urma_register_seg()
    
    Args:
        ctx: URMA上下文
        seg_cfg: 内存段配置
    
    Returns:
        内存段对象
    """
    pass

def urma_unregister_seg(ctx: 'UrmaContext', seg: 'UrmaSeg') -> None:
    """
    取消注册内存段
    对应UMDK: urma_unregister_seg()
    """
    pass

def urma_import_seg(ctx: 'UrmaContext', seg_info: bytes) -> 'UrmaRemoteSeg':
    """
    导入远程内存段
    对应UMDK: urma_import_seg()
    """
    pass

def urma_unimport_seg(ctx: 'UrmaContext', remote_seg: 'UrmaRemoteSeg') -> None:
    """
    取消导入内存段
    对应UMDK: urma_unimport_seg()
    """
    pass

# ============ 数据传输 ============

def urma_write(
    jfs: 'UrmaJfs',
    remote_jetty: 'UrmaRemoteJetty',
    local_seg: 'UrmaSeg',
    remote_seg: 'UrmaRemoteSeg',
    local_addr: int,
    remote_addr: int,
    length: int,
    flags: int = 0
) -> int:
    """
    RDMA写操作
    对应UMDK: urma_write()
    
    Args:
        jfs: 发送端Jetty
        remote_jetty: 远程Jetty
        local_seg: 本地内存段
        remote_seg: 远程内存段
        local_addr: 本地地址偏移
        remote_addr: 远程地址偏移
        length: 传输长度
        flags: 标志位
    
    Returns:
        工作请求ID (wr_id)
    """
    pass

def urma_read(
    jfs: 'UrmaJfs',
    remote_jetty: 'UrmaRemoteJetty',
    local_seg: 'UrmaSeg',
    remote_seg: 'UrmaRemoteSeg',
    local_addr: int,
    remote_addr: int,
    length: int,
    flags: int = 0
) -> int:
    """
    RDMA读操作
    对应UMDK: urma_read()
    
    Args:
        jfs: 发送端Jetty
        remote_jetty: 远程Jetty
        local_seg: 本地内存段
        remote_seg: 远程内存段
        local_addr: 本地地址偏移
        remote_addr: 远程地址偏移
        length: 传输长度
        flags: 标志位
    
    Returns:
        工作请求ID (wr_id)
    """
    pass

def urma_send(
    jfs: 'UrmaJfs',
    remote_jetty: 'UrmaRemoteJetty',
    local_seg: 'UrmaSeg',
    addr: int,
    length: int,
    flags: int = 0
) -> int:
    """
    发送操作
    对应UMDK: urma_send()
    """
    pass

def urma_recv(
    jfr: 'UrmaJfr',
    local_seg: 'UrmaSeg',
    addr: int,
    length: int,
    flags: int = 0
) -> int:
    """
    接收操作
    对应UMDK: urma_recv()
    """
    pass

# ============ 完成队列 ============

def urma_create_jfc(ctx: 'UrmaContext', size: int = 1024) -> 'UrmaJfc':
    """
    创建完成队列
    对应UMDK: urma_create_jfc()
    """
    pass

def urma_poll_jfc(jfc: 'UrmaJfc', max_cr: int = 1) -> List['UrmaCr']:
    """
    轮询完成队列
    对应UMDK: urma_poll_jfc()
    
    Args:
        jfc: 完成队列
        max_cr: 最大轮询数量
    
    Returns:
        完成记录列表
    """
    pass

def urma_rearm_jfc(jfc: 'UrmaJfc') -> None:
    """
    重新臂中断
    对应UMDK: urma_rearm_jfc()
    """
    pass

def urma_wait_jfc(jfc: 'UrmaJfc', timeout_ms: int = -1) -> int:
    """
    等待完成事件
    对应UMDK: urma_wait_jfc()
    """
    pass

# ============ 序列化 ============

def urma_serialize_seg(seg: 'UrmaSeg') -> bytes:
    """
    序列化内存段 (用于传输到远程)
    """
    pass

def urma_deserialize_seg(data: bytes) -> 'UrmaSegInfo':
    """
    反序列化内存段信息
    """
    pass

def urma_serialize_jetty_id(jetty_id: 'UrmaJettyId') -> bytes:
    """
    序列化Jetty ID
    """
    pass

def urma_deserialize_jetty_id(data: bytes) -> 'UrmaJettyId':
    """
    反序列化Jetty ID
    """
    pass
```

### 3.2 数据类型 (types.py)

```python
"""
URMA数据类型定义
"""

from dataclasses import dataclass
from typing import Optional
from enum import IntEnum

class UrmaTransportMode(IntEnum):
    """传输模式"""
    RM = 0  # Reliable Message
    RC = 1  # Reliable Connection
    UM = 2  # Unreliable Message

class UrmaAccessFlags(IntEnum):
    """访问权限标志"""
    LOCAL_ONLY = 0x1 << 0
    READ = 0x1 << 1
    WRITE = 0x1 << 2
    ATOMIC = 0x1 << 3
    REMOTE_READ = 0x1 << 4
    REMOTE_WRITE = 0x1 << 5

class UrmaCrStatus(IntEnum):
    """完成记录状态"""
    SUCCESS = 0
    LOCAL_LEN_ERR = 1
    LOCAL_OP_ERR = 2
    LOCAL_PROTECTION_ERR = 3
    REMOTE_RDMA_ERR = 4
    RETRY_EXCEEDED_ERR = 5
    REMOTE_ABORT = 6
    WR_FLUSH_ERR = 7

class UrmaTokenType(IntEnum):
    """Token类型"""
    NONE = 0
    PLAIN_TEXT = 1
    SIGNED = 2
    ALL_ENCRYPTED = 3

class UrmaDsvaFlag(IntEnum):
    """DSVA标志"""
    DISABLE = 0
    ENABLE = 1

@dataclass
class UrmaDevice:
    """URMA设备"""
    name: str
    guid: bytes  # 16 bytes GUID
    transport_type: str
    node_guid: bytes
    port_guid: bytes

@dataclass
class UrmaDeviceAttr:
    """设备属性"""
    max_qp: int
    max_cq: int
    max_mr: int
    max_mr_size: int
    max_qp_wr: int
    max_qp_rd_atom: int
    max_sge: int
    max_sge_rd: int

@dataclass
class UrmaSegCfg:
    """内存段配置"""
    va: int  # Virtual address
    len: int  # Length in bytes
    dsva: int = UrmaDsvaFlag.DISABLE  # DSVA flag
    access: int = UrmaAccessFlags.READ | UrmaAccessFlags.WRITE
    token_type: int = UrmaTokenType.NONE
    token: Optional[bytes] = None

@dataclass
class UrmaSeg:
    """已注册的内存段"""
    handle: int  # Internal handle
    va: int
    len: int
    lkey: int  # Local key
    rkey: int  # Remote key

@dataclass
class UrmaSegInfo:
    """内存段信息 (用于传输)"""
    va: int
    len: int
    rkey: int
    dsva_enabled: bool

@dataclass
class UrmaJettyId:
    """Jetty标识符"""
    eid: bytes  # Endpoint ID (24 bytes)
    jetty: int  # Jetty number
    token: Optional[bytes] = None

@dataclass
class UrmaJetty:
    """Jetty对象"""
    handle: int
    jfs: 'UrmaJfs'  # Sender part
    jfr: 'UrmaJfr'  # Receiver part
    jfc: 'UrmaJfc'  # Completion queue

@dataclass
class UrmaJfs:
    """发送端Jetty"""
    handle: int
    max_wr: int
    jfc: 'UrmaJfc'

@dataclass
class UrmaJfr:
    """接收端Jetty"""
    handle: int
    max_wr: int
    jfc: 'UrmaJfc'

@dataclass
class UrmaJfc:
    """完成队列"""
    handle: int
    size: int

@dataclass
class UrmaCr:
    """完成记录"""
    wr_id: int  # Work request ID
    status: UrmaCrStatus
    opcode: int
    len: int  # Transferred length
    vendor_err: int

@dataclass
class UrmaContext:
    """URMA上下文"""
    handle: int
    device: UrmaDevice

@dataclass
class UrmaTargetSeg:
    """目标段描述"""
    va: int
    len: int
    rkey: int

@dataclass
class UrmaWrSge:
    """工作请求SGE"""
    addr: int
    len: int
    lkey: int
```

### 3.3 高级封装 (agent.py)

```python
"""
UrmaAgent - 高级封装类
提供类似NIXL的简化API
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import torch

from .core import (
    urma_init, urma_uninit, urma_get_device_list,
    urma_create_context, urma_delete_context,
    urma_create_jetty, urma_delete_jetty,
    urma_register_seg, urma_unregister_seg,
    urma_import_seg, urma_unimport_seg,
    urma_import_jetty, urma_unimport_jetty,
    urma_get_jetty_id,
    urma_read, urma_write,
    urma_poll_jfc, urma_wait_jfc,
    urma_serialize_seg, urma_deserialize_seg,
)
from .types import (
    UrmaSegCfg, UrmaSeg, UrmaSegInfo, UrmaJettyId,
    UrmaAccessFlags, UrmaDsvaFlag, UrmaTokenType,
    UrmaCrStatus, UrmaTransportMode
)
from .errors import UrmaError, UrmaTimeoutError, UrmaTransferError

@dataclass
class UrmaTransferHandle:
    """传输句柄"""
    wr_id: int
    operation: str  # "read" or "write"
    local_seg: UrmaSeg
    remote_seg: UrmaSegInfo
    length: int

class UrmaAgent:
    """
    URMA代理类 - 类似NIXL的nixl_agent
    提供简化的内存注册、传输接口
    """
    
    def __init__(self, name: str = "default", device_index: int = 0):
        """
        初始化URMA代理
        
        Args:
            name: 代理名称
            device_index: 设备索引
        """
        self._name = name
        self._device_index = device_index
        self._ctx = None
        self._jetty = None
        self._jfc = None
        self._segs: Dict[int, UrmaSeg] = {}
        self._remote_jettys: Dict[str, Any] = {}
        self._remote_segs: Dict[str, UrmaSegInfo] = {}
        self._initialized = False
        self._wr_id_counter = 0
    
    def __enter__(self):
        """上下文管理器入口"""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.cleanup()
    
    def initialize(self) -> bool:
        """
        初始化代理
        
        Returns:
            是否成功
        """
        if self._initialized:
            return True
        
        # Initialize URMA
        if not urma_init():
            raise UrmaError("Failed to initialize URMA")
        
        # Get device
        devices = urma_get_device_list()
        if not devices or self._device_index >= len(devices):
            raise UrmaError(f"No device available at index {self._device_index}")
        
        device = devices[self._device_index]
        
        # Create context
        self._ctx = urma_create_context(device)
        
        # Create jetty (bidirectional endpoint)
        self._jetty = urma_create_jetty(
            self._ctx,
            transport_mode=UrmaTransportMode.RC,
            max_jfs=128,
            max_jfr=128,
            jfc_size=1024
        )
        
        self._initialized = True
        return True
    
    def cleanup(self) -> None:
        """清理资源"""
        # Unregister all segments
        for seg_handle in list(self._segs.keys()):
            self.unregister_memory(seg_handle)
        
        # Unimport all remote jettys
        for name in list(self._remote_jettys.keys()):
            self.remove_remote_agent(name)
        
        # Delete jetty
        if self._jetty:
            urma_delete_jetty(self._jetty)
            self._jetty = None
        
        # Delete context
        if self._ctx:
            urma_delete_context(self._ctx)
            self._ctx = None
        
        # Uninitialize URMA
        urma_uninit()
        
        self._initialized = False
    
    def register_memory(
        self,
        tensors: List[torch.Tensor],
        mem_type: Optional[str] = None
    ) -> List[int]:
        """
        注册张量内存
        
        Args:
            tensors: 张量列表
            mem_type: 内存类型 ("npu", "cuda", "cpu")，自动检测
        
        Returns:
            内存段句柄列表
        """
        if not self._initialized:
            raise UrmaError("Agent not initialized")
        
        handles = []
        
        for tensor in tensors:
            # Auto-detect memory type
            detected_type = mem_type or self._detect_memory_type(tensor)
            
            # Create segment config
            seg_cfg = UrmaSegCfg(
                va=tensor.untyped_storage().data_ptr(),
                len=tensor.untyped_storage().nbytes(),
                dsva=UrmaDsvaFlag.ENABLE if detected_type in ['npu', 'cuda'] else UrmaDsvaFlag.DISABLE,
                access=UrmaAccessFlags.READ | UrmaAccessFlags.WRITE | 
                       UrmaAccessFlags.REMOTE_READ | UrmaAccessFlags.REMOTE_WRITE,
                token_type=UrmaTokenType.SIGNED,
            )
            
            # Register segment
            seg = urma_register_seg(self._ctx, seg_cfg)
            
            # Cache segment
            self._segs[seg.handle] = seg
            handles.append(seg.handle)
        
        return handles
    
    def unregister_memory(self, seg_handle: int) -> None:
        """取消注册内存段"""
        if seg_handle in self._segs:
            urma_unregister_seg(self._ctx, self._segs[seg_handle])
            del self._segs[seg_handle]
    
    def get_xfer_descs(self, seg_handles: List[int]) -> List[bytes]:
        """
        获取传输描述符
        
        Args:
            seg_handles: 内存段句柄列表
        
        Returns:
            序列化的描述符列表
        """
        descs = []
        for handle in seg_handles:
            if handle not in self._segs:
                raise UrmaError(f"Segment {handle} not registered")
            seg = self._segs[handle]
            serialized = urma_serialize_seg(seg)
            descs.append(serialized)
        return descs
    
    def get_agent_metadata(self) -> bytes:
        """
        获取代理元数据 (用于交换到远程)
        
        Returns:
            序列化的元数据
        """
        if not self._initialized or not self._jetty:
            raise UrmaError("Agent not initialized")
        
        jetty_id = urma_get_jetty_id(self._jetty)
        return urma_serialize_jetty_id(jetty_id)
    
    def add_remote_agent(self, metadata: bytes) -> str:
        """
        添加远程代理
        
        Args:
            metadata: 远程代理元数据
        
        Returns:
            代理标识符
        """
        jetty_id = urma_deserialize_jetty_id(metadata)
        remote_jetty = urma_import_jetty(self._ctx, jetty_id)
        
        agent_id = f"remote_{len(self._remote_jettys)}"
        self._remote_jettys[agent_id] = remote_jetty
        
        return agent_id
    
    def remove_remote_agent(self, agent_id: str) -> None:
        """移除远程代理"""
        if agent_id in self._remote_jettys:
            urma_unimport_jetty(self._ctx, self._remote_jettys[agent_id])
            del self._remote_jettys[agent_id]
    
    def import_remote_segment(self, descriptor: bytes) -> str:
        """
        导入远程内存段
        
        Args:
            descriptor: 远程段描述符
        
        Returns:
            段标识符
        """
        seg_info = urma_deserialize_seg(descriptor)
        seg_id = f"remote_seg_{len(self._remote_segs)}"
        self._remote_segs[seg_id] = seg_info
        return seg_id
    
    def read(
        self,
        remote_agent: str,
        local_seg_handles: List[int],
        remote_seg_ids: List[str],
        timeout: float = 30.0
    ) -> bool:
        """
        RDMA READ操作 - 从远程读取数据到本地
        
        Args:
            remote_agent: 远程代理ID
            local_seg_handles: 本地内存段句柄
            remote_seg_ids: 远程内存段ID
            timeout: 超时时间(秒)
        
        Returns:
            是否成功
        """
        if remote_agent not in self._remote_jettys:
            raise UrmaError(f"Remote agent {remote_agent} not found")
        
        remote_jetty = self._remote_jettys[remote_agent]
        
        # Create transfer handles
        transfer_handles = []
        
        for local_handle, remote_id in zip(local_seg_handles, remote_seg_ids):
            if local_handle not in self._segs:
                raise UrmaError(f"Local segment {local_handle} not registered")
            if remote_id not in self._remote_segs:
                raise UrmaError(f"Remote segment {remote_id} not imported")
            
            local_seg = self._segs[local_handle]
            remote_seg = self._remote_segs[remote_id]
            
            # Create RDMA READ
            wr_id = self._next_wr_id()
            urma_read(
                self._jetty.jfs,
                remote_jetty,
                local_seg,
                remote_seg,
                local_addr=0,
                remote_addr=0,
                length=local_seg.len,
                flags=0
            )
            
            transfer_handles.append(UrmaTransferHandle(
                wr_id=wr_id,
                operation="read",
                local_seg=local_seg,
                remote_seg=remote_seg,
                length=local_seg.len
            ))
        
        # Wait for completion
        return self._wait_transfers(transfer_handles, timeout)
    
    def write(
        self,
        remote_agent: str,
        local_seg_handles: List[int],
        remote_seg_ids: List[str],
        timeout: float = 30.0
    ) -> bool:
        """
        RDMA WRITE操作 - 将本地数据写入远程
        """
        # Similar to read
        pass
    
    def check_xfer_state(self, handle: UrmaTransferHandle) -> str:
        """
        检查传输状态
        
        Args:
            handle: 传输句柄
        
        Returns:
            状态: "PROC", "DONE", "ERR"
        """
        crs = urma_poll_jfc(self._jetty.jfs.jfc, max_cr=1)
        
        if not crs:
            return "PROC"
        
        cr = crs[0]
        
        if cr.status == UrmaCrStatus.SUCCESS:
            return "DONE"
        else:
            return "ERR"
    
    def _wait_transfers(
        self,
        handles: List[UrmaTransferHandle],
        timeout: float
    ) -> bool:
        """等待传输完成"""
        import time
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_done = True
            for handle in handles:
                state = self.check_xfer_state(handle)
                if state == "PROC":
                    all_done = False
                    break
                elif state == "ERR":
                    raise UrmaTransferError(f"Transfer {handle.wr_id} failed")
            
            if all_done:
                return True
            
            time.sleep(0.001)
        
        raise UrmaTimeoutError(f"Transfer timeout after {timeout}s")
    
    def _detect_memory_type(self, tensor: torch.Tensor) -> str:
        """检测张量内存类型"""
        if hasattr(tensor, 'device'):
            device_str = str(tensor.device)
            if 'npu' in device_str:
                return 'npu'
            elif 'cuda' in device_str:
                return 'cuda'
        return 'cpu'
    
    def _next_wr_id(self) -> int:
        """生成下一个WR ID"""
        self._wr_id_counter += 1
        return self._wr_id_counter
```

### 3.4 错误定义 (errors.py)

```python
"""
URMA错误定义
"""

class UrmaError(Exception):
    """URMA基础错误"""
    pass

class UrmaInitError(UrmaError):
    """初始化错误"""
    pass

class UrmaMemoryError(UrmaError):
    """内存注册错误"""
    pass

class UrmaTransferError(UrmaError):
    """传输错误"""
    pass

class UrmaTimeoutError(UrmaError):
    """超时错误"""
    pass

class UrmaDeviceError(UrmaError):
    """设备错误"""
    pass

class UrmaConnectionError(UrmaError):
    """连接错误"""
    pass
```

## 四、使用示例

### 4.1 基本使用

```python
import torch
from pyurma import UrmaAgent

# 创建代理
agent = UrmaAgent(name="worker_0")

try:
    # 初始化
    agent.initialize()
    
    # 创建张量
    tensor = torch.randn(1024, 1024, device="npu:0")
    
    # 注册内存
    handles = agent.register_memory([tensor])
    
    # 获取传输描述符
    descs = agent.get_xfer_descs(handles)
    
    # 获取代理元数据 (发送给远程)
    metadata = agent.get_agent_metadata()
    
    # 接收方:
    # 添加远程代理
    remote_id = agent.add_remote_agent(remote_metadata)
    
    # 导入远程内存段
    seg_id = agent.import_remote_segment(remote_desc)
    
    # 执行RDMA READ
    success = agent.read(
        remote_agent=remote_id,
        local_seg_handles=handles,
        remote_seg_ids=[seg_id],
        timeout=30.0
    )
    
finally:
    # 清理
    agent.cleanup()
```

### 4.2 与Ray RDT集成

```python
# 在urma_tensor_transport.py中使用
from pyurma import UrmaAgent

class UrmaTensorTransport(TensorTransportManager):
    def __init__(self):
        self._agent = UrmaAgent()
    
    def extract_tensor_transport_metadata(self, tensors, obj_id, metadata):
        if not self._agent._initialized:
            self._agent.initialize()
        
        # 注册内存
        handles = self._agent.register_memory(tensors)
        
        # 获取描述符
        descs = self._agent.get_xfer_descs(handles)
        agent_meta = self._agent.get_agent_metadata()
        
        return UrmaTransportMetadata(...)
```

## 五、C扩展实现要点

### 5.1 pybind11绑定示例

```cpp
// pyurma_bindings.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "urma_api.h"

namespace py = pybind11;

PYBIND11_MODULE(_pyurma, m) {
    // 初始化
    m.def("urma_init", [](py::object config) {
        return urma_init(nullptr) == 0;
    });
    
    m.def("urma_uninit", &urma_uninit);
    
    // 设备管理
    m.def("urma_get_device_list", []() {
        urma_device_t devices[64];
        int count = urma_get_device_list(devices, 64);
        py::list result;
        for (int i = 0; i < count; i++) {
            result.append(UrmaDevice(devices[i]));
        }
        return result;
    });
    
    // 上下文
    m.def("urma_create_context", [](UrmaDevice device, int flags) {
        urma_context_t ctx = urma_create_context(device.handle, flags);
        return UrmaContext(ctx);
    });
    
    // 内存注册
    m.def("urma_register_seg", [](UrmaContext ctx, py::dict cfg) {
        urma_seg_cfg_t seg_cfg;
        seg_cfg.va = cfg["va"].cast<uint64_t>();
        seg_cfg.len = cfg["len"].cast<uint64_t>();
        seg_cfg.flag.bs.dsva = cfg["dsva"].cast<int>();
        seg_cfg.flag.bs.access = cfg["access"].cast<uint32_t>();
        
        urma_seg_t seg = urma_register_seg(ctx.handle, &seg_cfg);
        return UrmaSeg(seg);
    });
    
    // RDMA操作
    m.def("urma_read", [](UrmaJfs jfs, UrmaRemoteJetty remote_jetty,
                          UrmaSeg local_seg, UrmaRemoteSeg remote_seg,
                          uint64_t local_addr, uint64_t remote_addr,
                          uint64_t length, int flags) {
        urma_wr_t wr;
        wr.opcode = URMA_OPC_READ;
        wr.target_segs[0].va = remote_addr;
        wr.target_segs[0].len = length;
        wr.target_segs[0].rkey = remote_seg.rkey;
        wr.sg_list[0].addr = local_addr;
        wr.sg_list[0].len = length;
        wr.sg_list[0].lkey = local_seg.lkey;
        
        return urma_post_jfs_wr(jfs.handle, &wr);
    });
    
    // 类型绑定
    py::class_<UrmaSeg>(m, "UrmaSeg")
        .def_readonly("handle", &UrmaSeg::handle)
        .def_readonly("va", &UrmaSeg::va)
        .def_readonly("len", &UrmaSeg::len)
        .def_readonly("lkey", &UrmaSeg::lkey)
        .def_readonly("rkey", &UrmaSeg::rkey);
}
```

## 六、编译配置

### 6.1 setup.py

```python
from setuptools import setup, Extension
from pybind11.setup_helpers import Pybind11Extension, build_ext

ext_modules = [
    Pybind11Extension(
        "pyurma._pyurma",
        ["pyurma/bindings/pyurma_bindings.cpp"],
        include_dirs=[
            "/path/to/umdk/include",
            "/path/to/pybind11/include",
        ],
        library_dirs=["/path/to/umdk/lib"],
        libraries=["urma", "urpc"],
        extra_compile_args=["-std=c++17"],
    ),
]

setup(
    name="pyurma",
    version="1.0.0",
    packages=["pyurma"],
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    install_requires=["torch"],
)
```

## 七、测试

### 7.1 单元测试

```python
# tests/test_urma_agent.py
import pytest
import torch
from pyurma import UrmaAgent, UrmaError

def test_agent_init():
    """测试代理初始化"""
    agent = UrmaAgent(name="test")
    assert agent.initialize() == True
    agent.cleanup()

def test_register_memory():
    """测试内存注册"""
    agent = UrmaAgent()
    agent.initialize()
    
    tensor = torch.randn(100, 100, device="cpu")
    handles = agent.register_memory([tensor])
    
    assert len(handles) == 1
    agent.cleanup()

def test_get_xfer_descs():
    """测试获取传输描述符"""
    agent = UrmaAgent()
    agent.initialize()
    
    tensor = torch.randn(100, 100)
    handles = agent.register_memory([tensor])
    descs = agent.get_xfer_descs(handles)
    
    assert len(descs) == 1
    assert isinstance(descs[0], bytes)
    agent.cleanup()
```

---

*文档版本: 1.0*
*最后更新: 2025年*