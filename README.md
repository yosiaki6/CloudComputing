CloudComputing
==============
* Start HBase cluster
* Launch an instance with Amazon Linux AMI

## Installation at the linux instance

    sudo yum -y update
    sudo yum -y install git nginx
    wget https://go.googlecode.com/files/go1.2.1.linux-amd64.tar.gz
    sudo tar -C /usr/local -xzf go1.2.1.linux-amd64.tar.gz
    mkdir go
    echo "export GOPATH=$HOME/go" >> ~/.bash_profile
    echo "export GOROOT=/usr/local/go" >> ~/.bash_profile
    echo "PATH=\$PATH:\$GOROOT/bin" >> ~/.bash_profile
    source ~/.bash_profile
    go get github.com/go-sql-driver/mysql
    go get github.com/sdming/goh

## Edit nginx.conf

Open text editor

    sudo vim /etc/nginx/nginx.conf
    
Then edit a section *http -> server* to be like this.
The key are lines "include" and "fastcgi_pass"

    http {
        ...
        server {
            listen 80;
            server_name togo;
    
            location / {
              include     fastcgi.conf;
              fastcgi_pass    127.0.0.1:9001;
            }
        }
    }
  
## Get project source code
    
    git clone https://github.com/yosiaki6/CloudComputing.git
    
## Run

    sudo /usr/sbin/nginx
    go run ~/CloudComputing/gofront/start.go
    
The console will then block until you press Ctrl-C.

Now, you can navigate to this server's address.
    
