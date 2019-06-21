File System Fragmentation test (fsfrag-test)
============================================


How to compile
--------------

    % make

or

    % MPICC="<path_to_mpicc>" make



Embeded Tools
-------------

### fsfrag-write
Each process creates <nfiles> files in the <dir_path> directory and appends
<bsize> Bytes round robbin in each file until reaching <fsize> total size per
file. Batches are used to avoid hitting the limit of files opened
simultaneously. It accepts the following arguments:

    -b, --bsize=<bytes>        Size in bytes of each block [default: 131072 B]
    -f, --fsize=<bytes>        Size in bytes of each file [default: 16777216 B]
    -n, --nfiles=<N>           Files per process [default: 100]
    -o, --ofiles=<N>           Max simultaneous opened files [default: 200]
    -?, --help                 Give this help list
        --usage                Give a short usage message
    -V, --version              Print program version





