
'''
A simple script to stream data from a Enterprise Data Collector (EDC).

This script can "self-discover" the current set of active EDC streams, or they can be statically
configured in the EDC configuration file.

Currently, this script only handles normalized Activity Stream (ATOM) data formatted in XML.

TODO:
    [] Add support for original format (JSON).

'''

require_relative "./http_stream"
require "base64"    #Used for basic password encryption.
require "yaml"      #Used for configuration file management.
require "nokogiri"  #Used for parsing activity XML.
require "cgi"       #Used only for retrieving HTTP GET parameters and converting to a hash.
#require "nori"     #I tried to use this gem to transform XML to a Hash, but the IDE kept blowing up!


class EDC_Client

    attr_accessor :http, :machine_name, :user_name, :password_encoded, :url,
                  :streams, :data,
                  :storage, :out_box,
                  :database, :db_host, :db_port, :db_schema, :db_user_name, :db_password,
                  :activities

        STREAM_SEARCH_LIMIT = 20  #Used to limit how high we go searching for active streams.

    def initialize(config_file = nil)

        @data = ""
        @activities = Array.new

        if not config_file.nil? then
            getCollectorConfig(config_file)
        end

        if not @machine_name.nil? then
            @url = "https://" + @machine_name + ".gnip.com/data_collectors"
        end

        #Set up a HTTP object.
        @http = PtREST.new  #Historical API is REST based (currently).
        @http.url = @url  #Pass the URL to the HTTP object.
        @http.user_name = @user_name  #Set the info needed for authentication.
        @http.password_encoded = @password_encoded  #HTTP class can decrypt password.

        @streams = Array.new #of stream (ID,name) definitions.
        if not config_file.nil? then
            getStreamConfig(config_file)
        end

    end

    def getPassword
        #You may want to implement a more secure password handler.  Or not.
        @password = Base64.decode64(@password_encoded)  #Decrypt password.
    end

    def getCollectorConfig(config_file)

        config = YAML.load_file(config_file)

        #Account details.
        @machine_name = config["account"]["machine_name"]
        @user_name  = config["account"]["user_name"]
        @password_encoded = config["account"]["password_encoded"]

        if @password_encoded.nil? then  #User is passing in plain-text password...
            @password = config["account"]["password"]
            @password_encoded = Base64.encode64(@password)
        else
            @password = getPassword
        end

        #EDC configuration details.
        @storage = config["edc"]["storage"]
        @out_box = config["edc"]["out_box"]

        if @storage == "database" then #Get database connection details...
            @db_host = config["database"]["host"]
            @db_port = config["database"]["port"]
            @db_schema = config["database"]["schema"]
            @db_user_name = config["database"]["user_name"]
            @db_password  = config["database"]["password"]


            @database = PtDatabase.new(@db_host, @db_port, @db_schema, @db_user_name, @db_password)
            begin
                @database.connect
            rescue
                #No database connection, put your error handling here...
                p "Could not connect to database @ #{@db_host}!"
            end
        end
    end

    def getStreamConfig(config_file)

        config = YAML.load_file(config_file)

        #Load any configured streams.
        streams = config["streams"]

        if not streams.nil? then  #Load from configuration file.
            streams.each do |stream|
                #p stream
                @streams << stream
            end

        else #Nothing in configuration file?  Then go discover what is there.
            @streams = discoverDataCollectors
        end

        #TODO: load in publisher from name.
        @streams.each do |stream|
            stream['publisher'] = stream['Name'].split(' ')[0]
        end

    end


    '''
    Tours potential end-points and determines whether they are found or not.
    This method is called if there are no streams defined in the configuration file.
    If there are ANY streams defined, this method is not called.
    This method currently hits the "api_help" end-point to determine whether a stream exists or not.
    Uses the STREAM_SEARCH_LIMIT constant to limit how "high" it looks with stream IDs.
    For streams that are found, it populates a stream array with:
        ID: the numeric ID assigned to the stream.
        Name: based on the HTML "title", with punctuation characters dropped.
    '''
    def discoverDataCollectors

        print "Pinging end-points, looking for active streams..."

        1.upto(STREAM_SEARCH_LIMIT) do |i|

            print "."

            url = "https://" + @machine_name + ".gnip.com/data_collectors/<stream_num>/api_help"
            url_test = url
            url_test["<stream_num>"] = i.to_s
            #p url_test

            #Make HTTP Get request.
            @http.url = url_test
            response = @http.GET

            #Parse response either "Not found" or an active data stream page
            if response.body.downcase.include?("not found") then #move on to the next test URL.
                next
            end

            #If active, parse for <title> and load @streams

            doc = Nokogiri::HTML(response.body)
            #Look for title
            stream_title = doc.css("title")[0].text
            #Drop punctuation
            stream_title.gsub!(/\W+/," ")

            stream = Hash.new
            stream["ID"] = i
            stream["name"] = stream_title

            p "Found " + stream_title + " @ ID: " + i.to_s

            #Add to stream array.
            @streams << stream
        end

        @streams
    end

    '''
    Parses normalized Activity Stream XML.
    Parsing details here are driven by the current database schema used to store activities.
    If writing files, then just write out the entire activity payload to the file.
    '''
    def processResponseXML(activity)

        docXML = Nokogiri::XML.parse(activity)  {|config| config.noblanks}

        #Set some defaults.
        node = docXML.root
        content = ""
        id = ""
        posted_at = ""
        publisher = "not_found"
        body = ""

        tags = Array.new
        values = Array.new

        node = docXML.root

        content = node.to_s #Grab this activity content

        #Parse the publisher and ID, used for both file and database writing.
        id_parsed = false
        publisher_parsed = false
        node.children.each do |child_node|
            if child_node.name == "id" then
                id = child_node.inner_text
                id_parsed = true
            end
            if child_node.name == "source" then
                child_node.children.each do |sub_node|
                    if sub_node.name == "title" then
                        publisher = sub_node.text.split(" ")[0]
                        publisher_parsed = true
                    end
                end
            end
            break if id_parsed and publisher_parsed
        end

        #Storing as a file?  Then we are writing the entire activity payload with no need to parse out details.
        if @storage == "files" then #Write to the file.
            filename = "#{publisher}_#{id}.xml" #Create file name
            File.open(@out_box + "/" + filename, "w") do |new_file|
                begin
                    new_file.write(content)
                    p "Writing #{filename}"
                rescue
                    p "Error writing file: #{filename}"
                end
            end
        else #Storing in database, so do more parsing for payload elements that have been promoted to db fields.
            node.children.each do |sub_node|

                id = sub_node.inner_text if sub_node.name == "id"
                posted_at = sub_node.inner_text if sub_node.name == "created"

                if sub_node.name == "object" then
                    sub_node.children.each do |sub_node_child|
                        body = sub_node_child.inner_text if sub_node_child.name == "content"
                    end
                end

                if sub_node.name == "matching_rules" then
                    sub_node.children.each do |rules_node|
                        values << rules_node.inner_text if rules_node.name == "matching_rule"
                        tags << rules_node.attr('tag') if rules_node.name == "matching_rule" unless tags.include?(rules_node.attr('tag'))
                    end
                end
            end

           @database.storeActivityData(id, posted_at, content, body, publisher, values, tags)
        end
    end

    #Stream threads are directed to here, which links to the stream 'consume' method...
    def consumeStream(stream)
        stream[:feed].consume do |message|

            @activities << message #Add to end of array.
            #p "Queueing #{@activities.length} activities..."
            #puts "#{message}"
        end
    end

    #There is one thread for storing @activities, and it is calls this.
    def storeActivities
        while true
            while @activities.length > 0
                activity = @activities.shift  #FIFO, popping from start of array.
                processResponseXML(activity)
            end
            sleep (2)
        end
    end

    '''
    Establishes a data stream for each EDC stream.
    Loads streams in separate threads.

    '''
    def streamData

        threads = []  #Thread pool.

        #OK, create stream threads, one stream per thread, one thread per stream.
        #These streams all add to @activities via the consumeStream method.
        @streams.each do |stream|

            #Spin up streaming URL for this EDC stream.
            url = "https://#{@machine_name}.gnip.com/data_collectors/#{stream["ID"]}/stream.xml"
            p url

            #Create an EDC streaming instance for this stream.
            stream[:feed] = EDCStream.new(url, @user_name, @password)

            t = Thread.new {Thread.pass; consumeStream(stream)}  #This thread hosts the consumeStream method.

            #Start the thread, with some error handling.
            begin
                t.run
            rescue ThreadError => e
                p e.message
            rescue
                p "Error"
            end

            threads << t  #Add it to our pool (array) of threads.
        end


        #OK, add a thread for consuming from @activities.
        #This thread sends activities to the database.
        t = Thread.new {storeActivities}

        begin
            t.run
        rescue ThreadError => e
            p e.message
        rescue
            p "Error"
        end

        threads << t #Add it to our pool (array) of threads.

        threads.each do |t|
            begin
                t.join
            rescue ThreadError => e
                p e.message
            #rescue
            #    p "Error"
            end
        end

    end
end



#=======================================================================================================================
#Database class.

'''
This class is meant to demonstrate basic code for building a "database" class for use with the
PowerTrack set of example code.  It is written in Ruby, but in its present form hopefully will
read like pseudo-code for other languages.

One option would be to use (Rails) ActiveRecord for data management, but it seems that may abstract away more than
desired.

Having said that, the database was created (and maintained/migrated) with Rails ActiveRecord.
It is just a great way to create databases.

ActiveRecord::Schema.define(:version => 20130306234839) do

  create_table "activities", :force => true do |t|
      t.string   "native_id"
      t.text     "content"
      t.text     "body"
      t.string   "rule_value"
      t.string   "rule_tag"
      t.string   "publisher"
      t.datetime "created_at",               :null => false
      t.datetime "updated_at",               :null => false
      t.datetime "posted_at"
  end

end

The above table fields are a bit arbitrary.  I cherry picked some Tweet details and promoted them to be table fields.
Meanwhile the entire tweet is stored, in case other parsing is needed downstream.
'''
class PtDatabase
    require "mysql2"
    require "time"
    require "json"
    require "base64"

    attr_accessor :client, :host, :port, :user_name, :password, :database, :sql

    def initialize(host=nil, port=nil, database=nil, user_name=nil, password=nil)
        #local database for storing activity data...

        if host.nil? then
            @host = "127.0.0.1" #Local host is default.
        else
            @host = host
        end

        if port.nil? then
            @port = 3306 #MySQL post is default.
        else
            @port = port
        end

        if not user_name.nil?  #No default for this setting.
            @user_name = user_name
        end

        if not password.nil? #No default for this setting.
            @password = password
        end

        if not database.nil? #No default for this setting.
            @database = database
        end
    end

    #You can pass in a PowerTrack configuration file and load details from that.
    def config=(config_file)
        @config = config_file
        getSystemConfig(@config)
    end


    #Load in the configuration file details, setting many object attributes.
    def getSystemConfig(config)

        config = YAML.load_file(config_file)

        #Config details.
        @host = config["database"]["host"]
        @port = config["database"]["port"]

        @user_name = config["database"]["user_name"]
        @password_encoded = config["database"]["password_encoded"]

        if @password_encoded.nil? then  #User is passing in plain-text password...
            @password = config["database"]["password"]
            @password_encoded = Base64.encode64(@password)
        end

        @database = config["database"]["schema"]
    end


    def to_s
        "EDC object => " + @host + ":" + @port.to_s + "@" + @user_name + " schema:" + @database
    end

    def connect
        #TODO: need support for password!
        @client = Mysql2::Client.new(:host => @host, :port => @port, :username => @user_name, :database => @database )
    end

    def disconnect
        @client.close
    end

    def SELECT(sql = nil)

        if sql.nil? then
            sql = @sql
        end

        result = @client.query(sql)

        result

    end

    def UPDATE(sql)
    end

    def REPLACE(sql)
        begin
            result = @client.query(sql)
            true
        rescue
            false
        end
    end

    #NativeID is defined as an integer.  This works for Twitter, but not for other publishers who use alphanumerics.
    #Tweet "id" field has this form: "tag:search.twitter.com,2005:198308769506136064"
    #This function parses out the numeric ID at end.
    def getTwitterNativeID(id)
        native_id = Integer(id.split(":")[-1])
    end

    #Twitter uses UTC.
    def getPostedTime(time_stamp)
        time_stamp = Time.parse(time_stamp).strftime("%Y-%m-%d %H:%M:%S")
    end

    #Replace some special characters with an _.
    #(Or, for Ruby, use ActiveRecord for all db interaction!)
    def handleSpecialCharacters(text)

        if text.include?("'") then
            text.gsub!("'","_")
        end
        if text.include?("\\") then
            text.gsub!("\\","_")
        end

        text
    end

    '''
    storeActivity
    Receives an Activity Stream data point formatted in JSON.
    Does some (hopefully) quick parsing of payload.
    Writes to an Activities table.

    t.string   "native_id"
    t.text     "content"
    t.text     "body"
    t.string   "rule_value"
    t.string   "rule_tag"
    t.string   "publisher"
    t.string   "job_uuid"  #Used for Historical PowerTrack.
    t.datetime "posted_time"
    '''
    def storeActivityData(native_id, posted_at, content, body, publisher, rule_values, rule_tags)

        content = handleSpecialCharacters(content)
        body = handleSpecialCharacters(body)

        #See if this activity is already in the database.
        sql = "SELECT * FROM activities WHERE native_id = '#{native_id}'"

        result = SELECT(sql)

        exists = false
        #p result.num_rows
        result.each do |row|
            exists = true
            p "Activity #{native_id} already stored..."
        end

        if not exists then

            #Build SQL.
            sql = "REPLACE INTO activities (native_id, posted_at, content, body, rule_value, rule_tag, publisher, created_at, updated_at ) " +
                "VALUES ('#{native_id}', '#{posted_at}', '#{content}', '#{body}', '#{rule_values}','#{rule_tags}','#{publisher}', UTC_TIMESTAMP(), UTC_TIMESTAMP());"

            if not REPLACE(sql) then
                p "Activity not written to database: " + publisher + " | " + native_id
            else
                p "Activity WRITTEN to database: " + publisher + " | " + native_id
            end
        else
           p "Activity #{native_id} already in database..."
        end
    end

end #PtDB class.


#=======================================================================================================================
#A simple RESTful HTTP class for interacting with the EDC end-point.
#Future versions will most likely use an external PtREST object, common to all PowerTrack ruby clients.
class PtREST
    require "net/https"     #HTTP gem.
    require "uri"

    attr_accessor :url, :uri, :user_name, :password_encoded, :headers, :data, :data_agent, :account_name, :publisher

    def initialize(url=nil, user_name=nil, password_encoded=nil, headers=nil)
        if not url.nil?
            @url = url
        end

        if not user_name.nil?
            @user_name = user_name
        end

        if not password_encoded.nil?
            @password_encoded = password_encoded
            @password = Base64.decode64(@password_encoded)
        end

        if not headers.nil?
            @headers = headers
        end
    end

    def url=(value)
        @url = value
        if not @url.nil?
            @uri = URI.parse(@url)
        end
    end

    def password_encoded=(value)
        @password_encoded=value
        if not @password_encoded.nil? then
            @password = Base64.decode64(@password_encoded)
        end
    end

    #Fundamental REST API methods:
    def POST(data=nil)

        if not data.nil? #if request data passed in, use it.
            @data = data
        end

        uri = URI(@url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        request = Net::HTTP::Post.new(uri.path)
        request.body = @data
        request.basic_auth(@user_name, @password)
        response = http.request(request)
        return response
    end

    def PUT(data=nil)

        if not data.nil? #if request data passed in, use it.
            @data = data
        end

        uri = URI(@url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        request = Net::HTTP::Put.new(uri.path)
        request.body = @data
        request.basic_auth(@user_name, @password)
        response = http.request(request)
        return response
    end

    def GET(params=nil)
        uri = URI(@url)

        #params are passed in as a hash.
        #Example: params["max"] = 100, params["since_date"] = 20130321000000
        if not params.nil?
            uri.query = URI.encode_www_form(params)
        end

        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        request = Net::HTTP::Get.new(uri.request_uri)
        request.basic_auth(@user_name, @password)

        response = http.request(request)
        return response
    end

    def DELETE(data=nil)
        if not data.nil?
            @data = data
        end

        uri = URI(@url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        request = Net::HTTP::Delete.new(uri.path)
        request.body = @data
        request.basic_auth(@user_name, @password)
        response = http.request(request)
        return response
    end
end #PtREST class.

#=======================================================================================================================
if __FILE__ == $0  #This script code is executed when running this file.

    OptionParser.new do |o|
        o.on('-c CONFIG') { |config| $config = config}
        o.parse!
    end

    if $config.nil? then
        $config = "./EDC_Config_private.yaml"  #Default
    end

    p "Creating EDC Client object with config file: " + $config

    edc = EDC_Client.new($config)
    edc.streamData

    p "Exiting"


end

