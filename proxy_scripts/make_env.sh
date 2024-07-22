wget https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2
tar -xvf jemalloc-5.2.1.tar.bz2
cd jemalloc-5.2.1
./configure --enable-prof
make
make install