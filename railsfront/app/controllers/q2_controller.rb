class Q2Controller < ApplicationController

	TEAM_INFO_HEADER = "GiraffeLovers,2161-4638-6264"

	def q2
		user_id = params[:userid]
		tweet_time = params[:tweet_time]
		tweet_time = tweet_time[0,10] + "+" + tweet_time[11,8]
		
		#query_mysql(user_id, tweet_time)
		query_hbase(user_id, tweet_time)
	end

	def query_mysql(user_id, tweet_time)
		require 'mysql2'
		client = Mysql2::Client.new(:host => "localhost", :username => "root", :password => "", :database => "project")
		dataset = client.query("SELECT tweet_id FROM tweets WHERE user_id = '#{user_id}' AND tweet_time = '#{tweet_time}'")
		tweet_id = ""
		dataset.each{|data|
	    		tweet_id += data["tweet_id"].to_s + "\n"
        	}
        	render :text => TEAM_INFO_HEADER + "\n" + tweet_id
	end

	def query_hbase(user_id, tweet_time)
		require 'ok_hbase'
		conn = OkHbase::Connection.new(host: 'ec2-54-85-145-245.compute-1.amazonaws.com', port: 9090, auto_connect:true )
		tweets = conn.table('tweets')
		result = tweets.row(user_id + '|' + tweet_time)
		render :text => TEAM_INFO_HEADER + "\n" + result['tweet_id:'].split(";").join("\n") + "\n"
	end 

end
