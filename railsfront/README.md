# Cloud Phase 1

*Amazon Linux AMI* (HVM) 2013.09.2 - ami-e9a18d80

## Set up
Install prerequisites

    # Install git, rails, mysql, sqlite
    \curl -sSL https://get.rvm.io | bash - s stable
    source ~/.profile
    rvm install 1.9.3
    rvm alias create default 1.9.3
    sudo yum -y install rubygems sqlite-devel git gcc mysql-devel ruby-devel mysql
    gem install rails --no-ri --no-rdoc
    gem install mysql2 --no-ri --no-rdoc
    
Remember to change HBase server's address in a function `query_hbase` in `app/controller/q2_controller.rb`
    
## Choose between MySQL or HBase
Comment/uncomment a line in `app/controller/q2_controller.rb`

## Run on port 80
    rvmsudo rails s -p 80
