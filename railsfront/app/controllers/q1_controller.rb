class Q1Controller < ApplicationController

	def q1
	        require 'date'
	        render :text => "GiraffeLovers,2161-4638-6264\n"+Time.now.strftime("%Y-%m-%d %H:%M:%S")
	end

end
