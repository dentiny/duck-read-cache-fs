FROM ubuntu:22.04

RUN yes | unminimize

RUN apt update \
 && apt install -y \
    bison \
    clang-format \
    cmake \
    curl \
    dstat \
    flex \
    g++ \
    gdb \
    git \
    htop \
    libicu-dev \
    libreadline-dev \
    libssl-dev \
    locales \
    man \
    pkg-config \
    sudo \
    vim \
    zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8

