FROM ubuntu:20.04
WORKDIR /workdir
ENV DEBIAN_FRONTEND noninteractive
RUN apt update && apt install -y \
        build-essential \
        make clang-format-9 pkg-config g++ autoconf \
		libtool asciidoctor libkmod-dev libudev-dev \
		uuid-dev libjson-c-dev libkeyutils-dev pandoc \
		libhwloc-dev libgflags-dev libtext-diff-perl \
		bash-completion systemd wget git \
        libndctl-dev libdaxctl-dev cmake vim \
		&& apt clean
RUN wget https://github.com/pmem/pmdk/releases/download/1.11.1/pmdk-1.11.1.tar.gz && rm -rf pmdk-1.11.1 && tar zxf pmdk-1.11.1.tar.gz && cd pmdk-1.11.1 && make -j`nproc` && make install && \
	cd .. && rm -rf pmdk-1.11.1
COPY redis-benchmark.patch /workdir/
RUN wget https://github.com/redis/redis/archive/refs/tags/6.2.6.tar.gz && rm -rf redis-6.2.6 && tar zxf 6.2.6.tar.gz && cd redis-6.2.6 && \
	make -j`nproc` && cp src/redis-benchmark /workdir/ && \
	patch src/redis-benchmark.c /workdir/redis-benchmark.patch && \
	make -j`nproc` && cp src/redis-benchmark /workdir/pmedis-benchmark
RUN mkdir -p -m 0600 /root/.ssh && \
    ssh-keyscan -H github.com bitbucket.org >> /root/.ssh/known_hosts
RUN git clone git@github.com:memark-io/pmedis.git && cd pmedis && git submodule update --init --recursive && ./build.sh && cp ./test/*.so /usr/local/lib
COPY start_pmedis.sh pmedis*.conf benchmark*.sh /workdir/
ENTRYPOINT ["/bin/bash", "/workdir/start_pmedis.sh"]
CMD ["try_pmedis"]
