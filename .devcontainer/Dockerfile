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
    python3-pip \
    sudo \
    vim \
    zlib1g-dev

RUN locale-gen en_US.UTF-8

