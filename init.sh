#!/bin/sh

cd /home/hadoop

# Get code
/usr/bin/git clone https://github.com/yosiaki6/CloudComputing.git

# Get go
/usr/bin/wget https://go.googlecode.com/files/go1.2.1.linux-amd64.tar.gz
/bin/tar -C /usr/local -xzf go1.2.1.linux-amd64.tar.gz
sudo /bin/tar -C /usr/local -xzf go1.2.1.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
mkdir ~/gocode
export GOPATH=$HOME/gocode
go get github.com/sdming/goh

# Config vim
mkdir -p ~/.vim/autoload ~/.vim/bundle; \
curl -Sso ~/.vim/autoload/pathogen.vim \
    https://raw.github.com/tpope/vim-pathogen/master/autoload/pathogen.vim
/usr/bin/git clone https://github.com/phoorichet/vim-config.git
cd vim-config
/usr/bin/git submodule init
/usr/bin/git submodule update
cp .vimrc ~/.vimrc
cp -r .vim/bundle/* ~/.vim/bundle/

# Enable ll command
alias ll=’ls -l’

# Install byobu
sudo apt-get install byobu -y

# Start thrift
/home/hadoop/bin/hbase-daemon.sh start thrift

sudo sh -c "ulimit -n 999999"
