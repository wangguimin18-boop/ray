"""
URMA Tensor Transport for Ray RDT
华为灵衢UMDK URMA适配层 - 替代NIXL传输后端
"""
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import torch

from ray.experimental.rdt.tensor_transport import (
    CollectiveTensorTransport,
    TensorTransportManager,
    TensorTransportMetadata,
)
from ray.experimental.rdt.utils import register_tensor_transport

if TYPE_CHECKING:
    from ray.experimental.rdt.rdt_store import RDTStoreMetadata

logger = logging.getLogger(__name__)

# URMA constants - should be imported from pyurma when available
URMA_DSVA_ENABLE = 1
URMA_DSVA_DISABLE = 0
URMA_ACCESS_LOCAL_ONLY = 0x1 << 0
URMA_ACCESS_READ = 0x1 << 1
URMA_ACCESS_WRITE = 0x1 << 2
URMA_ACCESS_ATOMIC = 0x1 << 3
URMA_ACCESS_REMOTE_READ = 0x1 << 4
URMA_ACCESS_REMOTE_WRITE = 0x1 << 5

URMA_CR_SUCCESS = 0
URMA_CR_LOCAL_LEN_ERR = 1
URMA_CR_LOCAL_OP_ERR = 2
URMA_CR_REMOTE_RDMA_ERR = 3
URMA_CR_RETRY_EXCEEDED_ERR = 4
URMA_CR_REMOTE_ABORT = 5
URMA_CR_WR_FLUSH_ERR = 6

URMA_TM_RM = 0  # Reliable Message
URMA_TM_RC = 1  # Reliable Connection
URMA_TM_UM = 2  # Unreliable Message

URMA_TOKEN_NONE = 0
URMA_TOKEN_PLAIN_TEXT = 1
URMA_TOKEN_SIGNED = 2
URMA_TOKEN_ALL_ENCRYPTED = 3


@dataclass
class UrmaSegDesc:
    """URMA内存段描述符"""
    seg_handle: int  # urma_seg_t handle
    va: int  # Virtual address
    len: int  # Length in bytes
    mem_type: str  # "npu", "cuda", "cpu"
    dsva_enabled: bool  # DSVA flag
    access_flags: int  # Access permissions


@dataclass
class UrmaJettyInfo:
    """URMA Jetty信息"""
    jetty_id: bytes  # Serialized urma_jetty_id_t
    eid: bytes  # Endpoint ID
    is_jfs: bool  # Is sender only
    is_jfr: bool  # Is receiver only


@dataclass
class UrmaTransportMetadata(TensorTransportMetadata):
    """URMA传输元数据"""
    urma_seg_info: bytes  # Serialized urma_seg_t
    urma_jetty_info: bytes  # Serialized UrmaJettyInfo
    urma_context_eid: bytes  # EID information
    dsva_enabled: bool  # DSVA enabled
    token_policy: int  # Token security policy
    tensor_shapes: List[Tuple[int, ...]]
    tensor_dtypes: List[torch.dtype]
    tensor_device: str  # "npu:0", "cuda:0", "cpu"


class UrmaAgent:
    """
    URMA Agent - 封装URMA上下文和操作
    对应NIXL的nixl_agent
    """
    
    def __init__(self, name: str = "default"):
        self._name = name
        self._ctx = None
        self._jetty = None
        self._jfc = None  # Completion queue
        self._registered_segs: Dict[int, UrmaSegDesc] = {}
        self._remote_jettys: OrderedDict = OrderedDict()
        self._remote_segs: Dict[str, UrmaSegDesc] = {}
        self._initialized = False
        
    def initialize(self, device_name: Optional[str] = None) -> bool:
        """
        初始化URMA上下文
        对应: nixl_agent()
        """
        if self._initialized:
            return True
            
        try:
            # Import pyurma bindings (to be implemented)
            # from pyurma import (
            #     urma_init, urma_get_device_list,
            #     urma_create_context, urma_create_jetty,
            #     urma_create_jfc
            # )
            
            # Placeholder for actual implementation
            logger.info(f"[URMA] Initializing URMA agent: {self._name}")
            
            # Step 1: Initialize URMA
            # urma_init(None)
            
            # Step 2: Get device list
            # devices = urma_get_device_list()
            # if not devices:
            #     raise RuntimeError("No URMA devices available")
            
            # Step 3: Create context
            # device = device_name or devices[0]
            # self._ctx = urma_create_context(device, 0)
            
            # Step 4: Create jetty (bidirectional endpoint)
            # self._jetty = urma_create_jetty(self._ctx, URMA_TM_RC)
            
            # Step 5: Create completion queue
            # self._jfc = urma_create_jfc(self._ctx, 1024)
            
            self._initialized = True
            logger.info(f"[URMA] Agent initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"[URMA] Failed to initialize agent: {e}")
            return False
    
    def register_memory(
        self,
        tensors: List[torch.Tensor],
        mem_type: Optional[str] = None
    ) -> List[int]:
        """
        注册内存段到URMA
        对应: nixl_agent.register_memory([addr, size, gpu_id, meta])
        """
        if not self._initialized:
            raise RuntimeError("URMA agent not initialized")
        
        seg_handles = []
        
        for tensor in tensors:
            # Auto-detect memory type
            detected_type = mem_type or self._detect_memory_type(tensor)
            
            # Create segment config
            seg_cfg = {
                'va': tensor.untyped_storage().data_ptr(),
                'len': tensor.untyped_storage().nbytes(),
                'mem_type': detected_type,
                'dsva': URMA_DSVA_ENABLE if detected_type in ['npu', 'cuda'] else URMA_DSVA_DISABLE,
                'access': URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_REMOTE_READ | URMA_ACCESS_REMOTE_WRITE,
            }
            
            logger.debug(f"[URMA] Registering memory: va=0x{seg_cfg['va']:x}, len={seg_cfg['len']}, type={detected_type}")
            
            # Actual URMA call
            # from pyurma import urma_register_seg, urma_seg_cfg_t
            # 
            # cfg = urma_seg_cfg_t()
            # cfg.va = seg_cfg['va']
            # cfg.len = seg_cfg['len']
            # cfg.flag.bs.dsva = seg_cfg['dsva']
            # cfg.flag.bs.access = seg_cfg['access']
            # 
            # seg_handle = urma_register_seg(self._ctx, cfg)
            
            # Placeholder
            seg_handle = seg_cfg['va']  # Use va as handle for now
            
            # Cache segment info
            self._registered_segs[seg_handle] = UrmaSegDesc(
                seg_handle=seg_handle,
                va=seg_cfg['va'],
                len=seg_cfg['len'],
                mem_type=detected_type,
                dsva_enabled=(seg_cfg['dsva'] == URMA_DSVA_ENABLE),
                access_flags=seg_cfg['access']
            )
            
            seg_handles.append(seg_handle)
        
        logger.info(f"[URMA] Registered {len(seg_handles)} memory segments")
        return seg_handles
    
    def unregister_memory(self, seg_handles: List[int]) -> None:
        """
        取消注册内存段
        对应: nixl_agent.deregister_memory()
        """
        for handle in seg_handles:
            if handle in self._registered_segs:
                # from pyurma import urma_unregister_seg
                # urma_unregister_seg(self._ctx, handle)
                
                del self._registered_segs[handle]
                logger.debug(f"[URMA] Unregistered segment: {handle}")
        
        logger.info(f"[URMA] Unregistered {len(seg_handles)} memory segments")
    
    def get_transfer_descriptors(self, seg_handles: List[int]) -> List[bytes]:
        """
        获取传输描述符
        对应: nixl_agent.get_xfer_descs()
        """
        descriptors = []
        
        for handle in seg_handles:
            if handle not in self._registered_segs:
                raise ValueError(f"Segment {handle} not registered")
            
            seg_desc = self._registered_segs[handle]
            
            # Serialize segment info
            # from pyurma import urma_serialize_seg
            # serialized = urma_serialize_seg(handle)
            
            # Placeholder: create JSON-like descriptor
            import json
            descriptor = json.dumps({
                'seg_handle': seg_desc.seg_handle,
                'va': seg_desc.va,
                'len': seg_desc.len,
                'mem_type': seg_desc.mem_type,
                'dsva_enabled': seg_desc.dsva_enabled,
            }).encode('utf-8')
            
            descriptors.append(descriptor)
        
        return descriptors
    
    def get_agent_metadata(self) -> bytes:
        """
        获取代理元数据
        对应: nixl_agent.get_agent_metadata()
        """
        if not self._initialized:
            raise RuntimeError("URMA agent not initialized")
        
        # Get jetty info
        # from pyurma import urma_get_jetty_id
        # jetty_id = urma_get_jetty_id(self._jetty)
        
        import json
        metadata = json.dumps({
            'agent_name': self._name,
            'jetty_info': 'placeholder_jetty_id',
            'eid': 'placeholder_eid',
        }).encode('utf-8')
        
        return metadata
    
    def add_remote_agent(self, metadata: bytes) -> str:
        """
        添加远程代理
        对应: nixl_agent.add_remote_agent()
        """
        import json
        meta_dict = json.loads(metadata.decode('utf-8'))
        
        agent_name = meta_dict['agent_name']
        
        # Import remote jetty
        # from pyurma import urma_import_jetty
        # remote_jetty = urma_import_jetty(self._ctx, meta_dict['jetty_info'])
        
        self._remote_jettys[agent_name] = {
            'jetty_info': meta_dict['jetty_info'],
            'eid': meta_dict['eid'],
        }
        
        logger.info(f"[URMA] Added remote agent: {agent_name}")
        return agent_name
    
    def remove_remote_agent(self, agent_name: str) -> None:
        """
        移除远程代理
        对应: nixl_agent.remove_remote_agent()
        """
        if agent_name in self._remote_jettys:
            # from pyurma import urma_unimport_jetty
            # urma_unimport_jetty(self._ctx, self._remote_jettys[agent_name]['jetty'])
            
            del self._remote_jettys[agent_name]
            logger.info(f"[URMA] Removed remote agent: {agent_name}")
    
    def import_remote_segment(self, descriptor: bytes) -> UrmaSegDesc:
        """
        导入远程内存段
        对应: NIXL deserialize_descs
        """
        import json
        seg_info = json.loads(descriptor.decode('utf-8'))
        
        # from pyurma import urma_import_seg
        # remote_seg = urma_import_seg(self._ctx, seg_info)
        
        seg_desc = UrmaSegDesc(
            seg_handle=seg_info['seg_handle'],
            va=seg_info['va'],
            len=seg_info['len'],
            mem_type=seg_info['mem_type'],
            dsva_enabled=seg_info['dsva_enabled'],
            access_flags=0
        )
        
        return seg_desc
    
    def read(
        self,
        remote_agent: str,
        local_seg_handles: List[int],
        remote_segs: List[UrmaSegDesc],
        timeout: float = 30.0
    ) -> bool:
        """
        执行RDMA READ操作
        对应: nixl_agent.transfer(READ)
        """
        if not self._initialized:
            raise RuntimeError("URMA agent not initialized")
        
        if remote_agent not in self._remote_jettys:
            raise ValueError(f"Remote agent {remote_agent} not added")
        
        if len(local_seg_handles) != len(remote_segs):
            raise ValueError("Local and remote segment count mismatch")
        
        logger.info(f"[URMA] Starting READ from {remote_agent}, {len(local_seg_handles)} segments")
        
        # Create transfer operations
        for local_handle, remote_seg in zip(local_seg_handles, remote_segs):
            local_desc = self._registered_segs[local_handle]
            
            # from pyurma import urma_read, urma_post_jfs_wr, urma_poll_jfc
            # 
            # # Get JFS for sending read request
            # jfs = self._get_jfs()
            # 
            # # Create read operation
            # read_op = urma_read(
            #     jfs,
            #     self._remote_jettys[remote_agent]['jetty'],
            #     local_desc.seg_handle,
            #     remote_seg.seg_handle,
            #     local_desc.va,
            #     remote_seg.va,
            #     local_desc.len,
            #     0  # flags
            # )
            # 
            # # Post work request
            # urma_post_jfs_wr(jfs, read_op)
            
            logger.debug(f"[URMA] READ: local=0x{local_desc.va:x}, remote=0x{remote_seg.va:x}, len={local_desc.len}")
        
        # Wait for completion
        # return self._wait_completion(timeout)
        
        # Placeholder: simulate completion
        time.sleep(0.001)
        return True
    
    def write(
        self,
        remote_agent: str,
        local_seg_handles: List[int],
        remote_segs: List[UrmaSegDesc],
        timeout: float = 30.0
    ) -> bool:
        """
        执行RDMA WRITE操作
        对应: nixl_agent.transfer(WRITE)
        """
        if not self._initialized:
            raise RuntimeError("URMA agent not initialized")
        
        if remote_agent not in self._remote_jettys:
            raise ValueError(f"Remote agent {remote_agent} not added")
        
        logger.info(f"[URMA] Starting WRITE to {remote_agent}, {len(local_seg_handles)} segments")
        
        # Similar to read but with WRITE operation
        # from pyurma import urma_write
        # write_op = urma_write(...)
        
        time.sleep(0.001)
        return True
    
    def _wait_completion(self, timeout: float = 30.0) -> bool:
        """
        等待传输完成
        对应: nixl_agent.check_xfer_state()
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # from pyurma import urma_poll_jfc
            # crs = urma_poll_jfc(self._jfc, 1)
            # 
            # if len(crs) > 0:
            #     if crs[0].status == URMA_CR_SUCCESS:
            #         return True
            #     else:
            #         raise RuntimeError(f"Transfer failed with status: {crs[0].status}")
            
            time.sleep(0.001)
        
        raise TimeoutError(f"Transfer timeout after {timeout}s")
    
    def _detect_memory_type(self, tensor: torch.Tensor) -> str:
        """自动检测张量内存类型"""
        if hasattr(tensor, 'device'):
            device_str = str(tensor.device)
            if 'npu' in device_str:
                return 'npu'
            elif 'cuda' in device_str:
                return 'cuda'
        return 'cpu'
    
    def cleanup(self):
        """清理资源"""
        # Unregister all segments
        self.unregister_memory(list(self._registered_segs.keys()))
        
        # Remove all remote agents
        for agent_name in list(self._remote_jettys.keys()):
            self.remove_remote_agent(agent_name)
        
        # Delete jetty and context
        # from pyurma import urma_delete_jetty, urma_delete_context
        # urma_delete_jetty(self._jetty)
        # urma_delete_context(self._ctx)
        
        self._initialized = False
        logger.info("[URMA] Agent cleaned up")


class UrmaTensorTransport(TensorTransportManager):
    """
    URMA Tensor Transport - Ray RDT URMA后端
    替代NixlTensorTransport
    """
    
    def __init__(self):
        self._agent: Optional[UrmaAgent] = None
        self._registered_tensors: Dict[str, List[int]] = {}  # obj_id -> seg_handles
    
    @staticmethod
    def tensor_transport_backend() -> str:
        return "URMA"
    
    @staticmethod
    def supported_devices() -> List[str]:
        return ["npu", "cpu"]
    
    @staticmethod
    def supported_tensor_types() -> List[Any]:
        return [torch.Tensor]
    
    def extract_tensor_transport_metadata(
        self,
        tensors: List[torch.Tensor],
        obj_id: str,
        metadata: "RDTStoreMetadata",
        comm_metadata: Optional[Any] = None,
    ) -> UrmaTransportMetadata:
        """
        提取传输元数据
        对应: NixlTensorTransport.extract_tensor_transport_metadata()
        """
        # Initialize agent if needed
        if self._agent is None:
            self._agent = UrmaAgent(name=f"agent_{obj_id}")
            self._agent.initialize()
        
        # Synchronize device if needed
        for tensor in tensors:
            if tensor.is_cuda or (hasattr(tensor, 'device') and 'npu' in str(tensor.device)):
                # torch.cuda.synchronize() or torch.npu.synchronize()
                if hasattr(torch, 'npu') and 'npu' in str(tensor.device):
                    torch.npu.synchronize(tensor.device.index)
                elif tensor.is_cuda:
                    torch.cuda.synchronize(tensor.device.index)
        
        # Register memory
        seg_handles = self._agent.register_memory(tensors)
        self._registered_tensors[obj_id] = seg_handles
        
        # Get descriptors
        descriptors = self._agent.get_transfer_descriptors(seg_handles)
        
        # Get agent metadata
        agent_meta = self._agent.get_agent_metadata()
        
        # Create transport metadata
        transport_meta = UrmaTransportMetadata(
            urma_seg_info=b''.join(descriptors),
            urma_jetty_info=agent_meta,
            urma_context_eid=b'placeholder_eid',
            dsva_enabled=True,
            token_policy=URMA_TOKEN_SIGNED,
            tensor_shapes=[t.shape for t in tensors],
            tensor_dtypes=[t.dtype for t in tensors],
            tensor_device=str(tensors[0].device) if tensors else 'cpu',
        )
        
        logger.info(f"[URMA] Extracted metadata for {len(tensors)} tensors, obj_id={obj_id}")
        return transport_meta
    
    def send_multiple_tensors(
        self,
        obj_id: str,
        tensors: List[torch.Tensor],
        metadata: "RDTStoreMetadata",
        comm_metadata: Optional[Any] = None,
    ) -> None:
        """
        发送张量（URMA为单边传输，此方法可为空）
        对应: NixlTensorTransport.send_multiple_tensors()
        """
        # URMA uses RDMA READ from receiver side
        # Sender just needs to register memory (already done in extract_tensor_transport_metadata)
        logger.info(f"[URMA] Send registered for obj_id={obj_id}, waiting for receiver READ")
        pass
    
    def recv_multiple_tensors(
        self,
        obj_id: str,
        metadata: UrmaTransportMetadata,
        comm_metadata: Optional[Any] = None,
        target_buffers: Optional[List[torch.Tensor]] = None,
    ) -> Optional[List[torch.Tensor]]:
        """
        接收张量
        对应: NixlTensorTransport.recv_multiple_tensors()
        """
        if target_buffers is None:
            # Allocate buffers
            target_buffers = []
            for shape, dtype in zip(metadata.tensor_shapes, metadata.tensor_dtypes):
                device = metadata.tensor_device
                if 'npu' in device and hasattr(torch, 'npu'):
                    buf = torch.empty(shape, dtype=dtype, device=device)
                else:
                    buf = torch.empty(shape, dtype=dtype)
                target_buffers.append(buf)
        
        # Initialize agent if needed
        if self._agent is None:
            self._agent = UrmaAgent(name=f"recv_agent_{obj_id}")
            self._agent.initialize()
        
        # Add remote agent
        remote_agent = self._agent.add_remote_agent(metadata.urma_jetty_info)
        
        # Import remote segments
        import json
        seg_infos = []
        offset = 0
        for shape, dtype in zip(metadata.tensor_shapes, metadata.tensor_dtypes):
            seg_info_json = metadata.urma_seg_info[offset:offset+200]  # Placeholder
            try:
                seg_info = json.loads(seg_info_json.decode('utf-8').split('}')[0] + '}')
                seg_infos.append(seg_info)
            except:
                pass
            offset += 200
        
        remote_segs = [self._agent.import_remote_segment(json.dumps(s).encode()) for s in seg_infos]
        
        # Register local buffers
        local_handles = self._agent.register_memory(target_buffers)
        
        # Execute RDMA READ
        success = self._agent.read(remote_agent, local_handles, remote_segs)
        
        if success:
            logger.info(f"[URMA] Received {len(target_buffers)} tensors for obj_id={obj_id}")
            return target_buffers
        else:
            raise RuntimeError(f"Failed to receive tensors for obj_id={obj_id}")
    
    def abort_tensor_transport(self, obj_id: str) -> None:
        """中止传输"""
        if obj_id in self._registered_tensors:
            seg_handles = self._registered_tensors[obj_id]
            if self._agent:
                self._agent.unregister_memory(seg_handles)
            del self._registered_tensors[obj_id]
        logger.info(f"[URMA] Aborted transport for obj_id={obj_id}")
    
    def garbage_collect(self) -> None:
        """垃圾回收"""
        if self._agent:
            self._agent.cleanup()
            self._agent = None
        self._registered_tensors.clear()
        logger.info("[URMA] Garbage collected")


class NpuIpcTransport(TensorTransportManager):
    """
    NPU IPC Transport - 用于同节点NPU间通信
    对应CudaIpcTransport
    """
    
    @staticmethod
    def tensor_transport_backend() -> str:
        return "NPU_IPC"
    
    @staticmethod
    def supported_devices() -> List[str]:
        return ["npu"]
    
    @staticmethod
    def supported_tensor_types() -> List[Any]:
        return [torch.Tensor]
    
    def extract_tensor_transport_metadata(
        self,
        tensors: List[torch.Tensor],
        obj_id: str,
        metadata: "RDTStoreMetadata",
        comm_metadata: Optional[Any] = None,
    ) -> TensorTransportMetadata:
        """
        提取NPU IPC元数据
        对应: CudaIpcTransport.extract_tensor_transport_metadata()
        """
        # Check if torch_npu is available
        if not hasattr(torch, 'npu'):
            raise RuntimeError("torch_npu not available for NPU IPC")
        
        # Synchronize NPU
        for tensor in tensors:
            if 'npu' in str(tensor.device):
                torch.npu.synchronize(tensor.device.index)
        
        # Get NPU IPC handles
        # Note: This requires torch_npu IPC support
        # Similar to CUDA IPC but for NPU
        
        ipc_handles = []
        events = []
        
        for tensor in tensors:
            # Create interprocess event
            # event = torch.npu.Event(interprocess=True)
            # torch.npu.current_stream(tensor.device).record_event(event)
            # event_handle = event.ipc_handle()
            # events.append(event)
            # ipc_handles.append(event_handle)
            pass
        
        # For now, use placeholder
        # Actual implementation requires torch_npu IPC API
        
        from dataclasses import dataclass
        @dataclass
        class NpuIpcMetadata(TensorTransportMetadata):
            ipc_handles: List[bytes]
            tensor_shapes: List[Tuple[int, ...]]
            tensor_dtypes: List[torch.dtype]
            tensor_device: str
        
        return NpuIpcMetadata(
            ipc_handles=ipc_handles,
            tensor_shapes=[t.shape for t in tensors],
            tensor_dtypes=[t.dtype for t in tensors],
            tensor_device=str(tensors[0].device) if tensors else 'npu:0',
        )
    
    def recv_multiple_tensors(
        self,
        obj_id: str,
        metadata: TensorTransportMetadata,
        comm_metadata: Optional[Any] = None,
        target_buffers: Optional[List[torch.Tensor]] = None,
    ) -> Optional[List[torch.Tensor]]:
        """
        接收NPU IPC张量
        """
        # Similar to CUDA IPC but for NPU
        # Requires torch_npu IPC API
        
        if target_buffers is None:
            # Allocate buffers
            target_buffers = []
            for shape, dtype in zip(metadata.tensor_shapes, metadata.tensor_dtypes):
                buf = torch.empty(shape, dtype=dtype, device=metadata.tensor_device)
                target_buffers.append(buf)
        
        # Wait for events
        # for event_handle in metadata.ipc_handles:
        #     event = torch.npu.Event.from_ipc_handle(event_handle)
        #     torch.npu.current_stream().wait_event(event)
        
        return target_buffers
    
    def send_multiple_tensors(self, obj_id: str, tensors: List[torch.Tensor], 
                               metadata: "RDTStoreMetadata", comm_metadata: Optional[Any] = None) -> None:
        pass
    
    def abort_tensor_transport(self, obj_id: str) -> None:
        pass
    
    def garbage_collect(self) -> None:
        pass


class CamCollectiveTransport(CollectiveTensorTransport):
    """
    CAM Collective Transport - 使用华为CAM算子进行集合通信
    替代NCCL后端
    """
    
    @staticmethod
    def tensor_transport_backend() -> str:
        return "CAM"
    
    @staticmethod
    def supported_devices() -> List[str]:
        return ["npu"]
    
    def recv_multiple_tensors(
        self,
        obj_id: str,
        metadata: TensorTransportMetadata,
        comm_metadata: Optional[Any] = None,
        target_buffers: Optional[List[torch.Tensor]] = None,
    ) -> Optional[List[torch.Tensor]]:
        """
        使用CAM算子接收张量
        对应: NCCLTensorTransport.recv_multiple_tensors()
        """
        # Get communicator info
        # from cam import SyncCollectives
        
        # sync = SyncCollectives()
        # sync.Init(comm_metadata.rank, comm_metadata.rank_size, ...)
        # sync.WaitSyncFlag(...)
        
        # Use CAM MOE or other collective operators
        # For point-to-point, use aclnn_moe_dispatch/combine
        
        raise NotImplementedError("CAM collective transport not yet implemented")
    
    def send_multiple_tensors(
        self,
        obj_id: str,
        tensors: List[torch.Tensor],
        metadata: "RDTStoreMetadata",
        comm_metadata: Optional[Any] = None,
    ) -> None:
        raise NotImplementedError("CAM collective transport not yet implemented")


def register_urma_transports():
    """注册URMA相关传输后端"""
    register_tensor_transport(
        "URMA",
        ["npu", "cpu"],
        UrmaTensorTransport,
        torch.Tensor
    )
    
    register_tensor_transport(
        "NPU_IPC",
        ["npu"],
        NpuIpcTransport,
        torch.Tensor
    )
    
    register_tensor_transport(
        "CAM",
        ["npu"],
        CamCollectiveTransport,
        torch.Tensor
    )


# Auto-register when module is imported
# register_urma_transports()