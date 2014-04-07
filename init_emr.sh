#!/bin/sh

cd /home/hadoop

# Get go
/usr/bin/wget https://go.googlecode.com/files/go1.2.1.linux-amd64.tar.gz
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

# Install byobu
sudo apt-get install byobu -y

# Get code
cd /home/hadoop
/usr/bin/git clone https://github.com/yosiaki6/CloudComputing.git

# Get vertx
/usr/bin/wget http://dl.bintray.com/vertx/downloads/vert.x-2.1RC3.tar.gz
sudo /bin/tar -C /usr/local -xzf vert.x-2.1RC3.tar.gz
sudo mv /usr/local/vert.x-2.1RC3 /usr/local/vertx
export PATH=$PATH:/usr/local/vertx/bin

# Copy necessary jars for vertx
sudo cp /home/hadoop/CloudComputing/HBaseBackend/lib/*.jar /usr/local/vertx/lib