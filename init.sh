#!/bin/sh

# Get code
git clone https://github.com/yosiaki6/CloudComputing.git

# Get go
wget https://go.googlecode.com/files/go1.2.1.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.2.1.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.2.1.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
mkdir ~/gocode
export GOPATH=$HOME/gocode
go get github.com/sdming/goh

# Config vim
mkdir -p ~/.vim/autoload ~/.vim/bundle; \
curl -Sso ~/.vim/autoload/pathogen.vim \
    https://raw.github.com/tpope/vim-pathogen/master/autoload/pathogen.vim
git clone https://github.com/phoorichet/vim-config.git
cd vim-config
git submodule init
git submodule update
cp .vimrc ~/.vimrc
cp -r .vim/bundle/* ~/.vim/bundle/

# Enable ll command
alias ll=’ls -l’