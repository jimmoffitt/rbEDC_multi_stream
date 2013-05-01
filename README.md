Introduction
============

This is a Ruby script written to stream multiple data feeds (up to 6!) from an Enterprise Data Collector (EDC). Retrieved data can be written to a local database or as individual activity files.

HTTP streaming is based on the gnip-stream project at https://github.com/rweald/gnip-stream.

EDC data streams can be specified in a configuration file.  Otherwise, there is a script method that can "discover" what streams are hosted on your EDC.  If you do not configure streams explicitly in your configuration file, the "discovery" method will be automatically triggered every time the script is launched.  

Once the script is started, it creates a separate thread for each EDC data feed.  There is yet another thread that processes the collected data and either writes out files or inserts into a database.

* Important note: this script is designed to process normalized Activity Streams (atom) data in XML.  


Usage
=====

One file is passed in at the command-line if you are running this code as a script (and not building some wrapper
around the EDC_client class):

1) A configuration file with account/username/password details and processing options (see below, or the sample project
file, for details):  -c "./EDC_Config.yaml"

The EDC configuration file needs to have an "account" section and a "edc" section.  If you specify that
you are using database (edc --> storage: database) you will need to have a "database" section as well.

So, if you were running from a directory with this source file in it, with the configuration file in that folder too,
the command-line would look like this:

        $ruby ./EDC_client_multi_stream.rb -c "./EDC_Config.yaml"


Configuration
=============

See the sample EDC_config.yaml file for an example of a EDC client configuration file.  

Here are some important points:

<p>
+ In the "account" section, you specify the "machine name" used in the URL for your EDC.  EDCs have the following URL pattern:
    https://machine_name.gnip.com

<p>
+ In the "edc" section, you can specify the following processing options:
	+ storage: "files" or "database".  How do you plan on storing the data? In flat files or in a database.
		If you are storing as files, the filename is based on the publisher and native activity "id" and the extension indicates the 
		markup format (xml or json, although only xml is currently supported). 
	+ out_box: If storing data in files, where do you want them written to?

<p>
+ In the "streams" section you have the option to explicitly list the EDC streams you want to collect data from. For each stream 
	you need to specify its "ID" and provide a stream name:
	
	+ ID: the numeric ID assigned to the stream.  This ID can be referenced by navigating to the data stream with the EDC dashboard and noting the numeric ID in the URL, as in "https://myEDC.gnip.com/data_collectors/**5**.  Note that these stream IDs are not always consecutive, and there will be gaps in the ID sequence if you have deleted any streams during the life of your EDC. 
		
	+ Name: a label given to the stream to help you identify the stream in the configuration file.  This name is echoed in standard output as the script runs.

<p>
* Example "streams" configuration section:

<code>

	streams:	
	  - ID 	  : 1
	    Name  : Facebook Keyword Search  
	  - ID    : 3
    	Name  : Google Plus Keyword Search
</code>