# ray
蓝区学习总结
person-wgm 分支的内核问题
三个内核文件都使用了 dcciCacheline()：
// bidir_put_kernel.cpp:50, bidir_get_kernel.cpp:48, bidir_put_packet_kernel.cpp:54
while (*inboundToken_ < expected) {
    dcciCacheline(reinterpret_cast<__gm__ uint8_t*>(inboundToken_));
}
// 以及 completionFlag 写入后:
*completionFlag_ = 1;
dcciCacheline(reinterpret_cast<__gm__ uint8_t*>(completionFlag_));
dcciCacheline 不是 AscendC 标准 API。CANN SDK 中不存在这个函数名。编译器找不到它，就会报 asc_dcci_single 未定义（因为 bisheng 编译器内部会将 dcciCacheline 映射到 CCE intrinsic asc_dcci_single，但如果映射失败或原函数不存在，就会在这个层面报错）。
与 npu 分支对比
npu` 分支（06-npu-memory-channel，验证成功的）的内核完全没有使用 DCCI：
- npuSimplePutKernel/npuSimpleGetKernel 直接使用 `MemoryChannelDeviceHandle.put<>/get<>() 框板函数拷贝
- semaphore.poll()/wait()/signal() 在 NPU 上是 空壳（no-op） — 因为跨设备可见性由 HCCS 硬件保证，不需要软件层的 cache flush
- 没有任何 dcciCacheline 调用
根本原因
Ascend 910B3 (dav-c220) 的 HCCS 硬件本身保证了跨 NPU 内存的一致性，不需要软件 DCCI flush。person-wgm 分支错误地假设需要像 CUDA 那样手动 flush cache line，但：
1. dcciCacheline 不是合法的 AscendC API — 编译器找不到它
2. 即使有对应的 CCE intrinsic (asc_dcci_single)，在 KERNEL_TYPE_AIV_ONLY 模式下也可能不可用
3. 在 HCCS 连接的 NPU 之间，硬件保证一致性，DCCI 操作是多余的
修复方向
去掉所有 dcciCacheline 调用。参考 npu 分支的做法，改用 MemoryChannelDeviceHandle 的 put/get 模板函数拷贝，用 PipeBarrier<PIPE_ALL>() 做 block 间同步。信号等待用 hostSema->signal()` 由 host 中转即可，kernel 内不需要自己轮询 semaphore token。
