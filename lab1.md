# Lab 1

实现直接参考[论文](resource/mapreduce.pdf)第三节 Implementation 部分即可.

## 执行流程

具体执行流程其实分为七步:

用户程序中的 MapReduce 库首先将输入文件拆分为 M 块, 通常为每块 16-64MB(可由用户通过可选参数控制). 然后它会在一组机器上启动该程序的许多副本.

> 1. The MapReduce library in the user program first splits the input files into M pieces of typically 16 megabytes to 64 megabytes(MB) per piece (controllable by the user via an optional parameter). It then starts up many copies of the program on a cluster of machines.

其中一个副本会成为 master, 其余的则成为 worker. 有 M 个 map 任务和 R 个 reduce 任务要分配. master 挑选空闲的 worker 并为每个 worker 分配一个 map 任务或一个 reduce 任务.

> 2. One of the copies of the program is special – the master. The rest are workers that are assigned work by the master. There are M map tasks and R reduce tasks to assign. The master picks idle workers and assigns each one a map task or a reduce task.

被分配了 map 任务的 worker 读取相应分片数据, 解析出键值对. 并将键值对一一传递给用户定义的 Map 函数. Map 函数产生的中间键值对数据缓存在内存中.

> 3. A worker who is assigned a map task reads the contents of the corresponding input split. It parses key/value pairs out of the input data and passes each pair to the user-defined Map function. The intermediate key/value pairs produced by the Map function are buffered in memory.

map worker 定期将内存中缓存的数据写入本地磁盘, 通过相同的分区函数划分为 R 个区域. 这些缓存对在本地磁盘上的位置被传递回 master, master 负责将这些位置转发给化 reduce worker.

> 4. Periodically, the buffered pairs are written to local disk, partitioned into R regions by the partitioning function. The locations of these buffered pairs on the local disk are passed back to the master, who is responsible for forwarding these locations to the reduce workers.

当 master 将这些位置通知给 reduce worker 时, master 会使用 rpc 从 map worker 的本地磁盘读取缓冲数据. 当 reduce worker 读取所有中间数据时, 它会按中间数据其进行排序, 以便将所有出现的相同 key 组合在一起. 排序是必需的, 因为通常许多不同的键映射到同一个 reduce 任务. 如果中间数据量太大而无法放入内存, 则使用外部排序.

> 5. When a reduce worker is notified by the master about these locations, it uses remote procedure calls to read the buffered data from the local disks of the map workers. When a reduce worker has read all intermediate data, it sorts it by the intermediate keys so that all occurrences of the same key are grouped together. The sorting is needed because typically many different keys map to the same reduce task. If the amount of intermediate data is too large to fit in memory, an external sort is used.

reduce worker 遍历排序后的中间数据, 对于遇到的每个唯一的中间键, 它将键和相应的中间值集传递给用户的 Reduce 函数. Reduce 函数的输出被附加到这个 reduce 分区的最终输出文件中.

> 6. The reduce worker iterates over the sorted intermediate data and for each unique intermediate key encountered, it passes the key and the correspondingset of intermediate values to the user’s Reduce function. The output of the Reduce function is appended to a final output file for this reduce partition.
