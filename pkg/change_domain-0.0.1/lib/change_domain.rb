# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "/net/http"
requrie "uri"

class LogStash::Filters::ChangeDomain < LogStash::Filters::Base

  config_name "change_domain"

  config :get_sequences_api, :validate => :string, :required=>true
  config :domain_field, :validate=> :string,:default=>"domain", :required=>false
  config :date_field, :validate=> :string, :default=>"date", :required=>false
  config :rc_seq, :validate=> :string, :default=>"17", required=>false
  config :vrc_seq, :validate=>:string, :default=>"7001", required=>false
  config :svr_seq, :validate=>:string, :default=>"3001", required=>false
  config :idc_node_id, :validate=>:string, default=>"AM", required=>false
  config :rc_id, :validate=>:string, :default=>"AM-07", required=>false
  config :vrc_id, :validate=>:string, :defualt=>"vrc7001.am-07", required=>false
  config :src_id, :validate=>:string, :default=>"fhs0001.am-07"
  
  @@domainData={}
  
  
  public
  def register
    @get_sequences_url = URI::parse(@get_sequece_api)
    @@domainData = { "d3lz1h0huhwnph.cloudfront.net" => {"sp_seq" => 1222, "svc_seq"=>1260, "vol_seq"=>1}}
  end

  public 
  def filter(event)
    event["rc_seq"]= @rc_seq
    event["vrc_seq"]=@vrc_seq
    event["svr_seq"]=@svr_seq
    event["idc_node_id"]=@idc_node_id
    event["rc_id"]=@rc_id
    event["vrc_id"]=@vrc_id
    event["svr_id"]=@svr_id
    
    foramtingDate()
    if((data = get_sequence(event[@domain_field])) != nil)
      data.each{ |key,value|
          event[key] = value
        }
    elseif((data = get_sequence_from_api(event[@domain_field])) != nil)
        data.each{ |key,value|
        event[key] =value
      }
      @@domainData[domain]=data
    else
         raise "Domain does not exist"
    end
    filter_matched(event);
  end
  
  
  ################################################
  # Change date format to statdate type
  ################################################
  private
  def foramtingDate()
    t = Time.new(event['year'],event['month'],event['day'],event['hour'],event['minutes'],event['seconds'])
    delete('year')
    delete('month')
    delete('day')
    delete('hour')
    delete('minutes')
    delete('seconds')
    
    t = t + (3600*9)
    event['statdate'] = t.strftime('%Y%m%d%H%M%S')
  end
  
  private
  def get_sequences_from_map(domain)
    if(domain != nil)
      data = @@domainData[domain];
      return data
    end
    return nil
  end
  
  #################################################
  # Get sequences from api
  # Timeout : 1 second 
  # Retry_num : 10 times
  #################################################
  private
  def get_sequeces_from_api(domain)
    retry_num = 0
    if(domain != nil)
      http = NET::HTTP.new(@get_sequences_url.host,@get_sequences_url.post)
      http.read_timeout=1
      begin
        data = JSON.parse(Net::HTTP.post_form(URL.parse(@get_sequences_api), 'domain'=>domain))
      rescue Timeout::Error =>e
        ++retry_num
        puts "Timeout"+e.message
        if(retry_num <10)
          retry  
        else
          raise e
        end
      end
      return data
    end
  end
end