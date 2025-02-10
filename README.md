This is an extension to the LeanStore Storage Engine which has been implemented here ([LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf)) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

Here, we are introducing Namespace-oriented recovery to recover namespaces based on priority. In a nutshell, we are isolating WAL entries of each namespace to achieve this segregation so that we can recover the namespaces on priority basis

## Compiling

Install dependencies:

`sudo apt-get install cmake libtbb2-dev libaio-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev liblmdb-dev libwiredtiger-dev liburing-dev`

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j && cd ..`

### Create a 10GB file named 'leanstore_ssd_file'
`dd if=/dev/zero of=leanstore_ssd_file bs=1G count=10`

### Give permissions
`chmod 777 ./leanstore_ssd_file`


## Cite

LeanStore was originally implemented using Pointer Swizzling ([paper](https://15721.courses.cs.cmu.edu/spring2020/papers/23-largethanmemory/leis-icde2018.pdf)).
More recently, LeanStore was also completely rewritten from scratch, replacing Pointer Swizzling with Virtual-Memory Assisted Buffer Pool ([Paper](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/_my_direct_uploads/vmcache.pdf)).
