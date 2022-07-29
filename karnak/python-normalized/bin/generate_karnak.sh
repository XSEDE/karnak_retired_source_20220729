#!/bin/bash

HOME=/home/karnak

export PYTHONPATH=$HOME/pkg/mtk/lib:$HOME/pkg/ipf/lib:$HOME/carnac/lib

export PATH=$HOME/pkg/karnakenv/bin:$PATH

python26 $HOME/carnac/libexec/generate_karnak.py
