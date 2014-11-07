# encoding: utf-8
require 'date'
require 'json'
require 'bunny'
require 'mysql2'
require 'logger'
require 'cassandra'

class LenzBase
  def initialize(show_query = false)
    initLogger
    @sql_clients = {}
    @cql_clients = {}
    @mq_sessions = {}
    @mq_channels = {}
    @show_query = show_query
  end

  def initLogger
    @logger = Logger.new(STDOUT)
    @logger.datetime_format="%Y-%m-%d %H:%M:%S"
  end

  def log(msg)
    @logger.info(msg)
  end

  def sleep(duration)
    log("sleep #{duration}s")
    Kernel.sleep(duration)
  end

  def getSqlClient(db_info)
    if(!@sql_clients[db_info])
      @sql_clients[db_info] = createSqlClient(db_info)
    end
    return @sql_clients[db_info]
  end

  def removeSqlClient(db_info)
    if(@sql_clients[db_info])
      @sql_clients[db_info].close
    end
    @sql_clients[db_info] = nil
  end

  def createSqlClient(db_info)
    begin
      client = Mysql2::Client.new(db_info)
      log("MySql connected #{db_info}")
      return client
    rescue Exception => e
      log e
      sleep(5)
      retry
    end
  end

  def querySql(db_info, query)
    begin
      result = getSqlClient(db_info).query(query)
      log("SQL query launched for #{db_info}")
      log(query) if @show_query
      return result ? result.entries : nil
    rescue Exception => e
      log e
      removeSqlClient(db_info)
      sleep(5)
      retry
    end
  end

  def getCqlClient(db_info)
    if(!@cql_clients[db_info])
      @cql_clients[db_info] = createCqlClient(db_info)
    end
    return @cql_clients[db_info]
  end

  def removeCqlClient(db_info)
    if(@cql_clients[db_info])
      @cql_clients[db_info].close
    end
    @cql_clients[db_info] = nil
  end

  def createCqlClient(db_info)
    begin
      client = Cassandra.cluster(db_info).connect(db_info['key_space'])
      log("Cassandra connected #{db_info}")
      return client
    rescue Exception => e
      log e
      sleep(5)
      retry
    end
  end

  def queryCql(db_info, query)
    begin
      result = getCqlClient(db_info).execute(query)
      log("CQL query launched for #{db_info}")
      log(query) if @show_query
      return result
    rescue Exception => e
      log e
      removeCqlClient(db_info)
      sleep(5)
      retry
    end
  end

  def getMQChannel(mq_info)
    if(!@mq_channels[mq_info] || !@mq_channels[mq_info].active || !@mq_sessions[mq_info].connected?)
      @mq_channels[mq_info] = openMQChannel(mq_info)
    end
    return @mq_channels[mq_info]
  end

  def openMQChannel(mq_info)
    begin
      if(!@mq_sessions[mq_info] || !@mq_sessions[mq_info].connected?)
        @mq_sessions[mq_info] = Bunny.new(mq_info)
        @mq_sessions[mq_info].start
        log("Message Queue connected #{mq_info}")
      end

      if(@mq_sessions[mq_info].respond_to? :default_channel)
        return @mq_sessions[mq_info].default_channel
      else
        return @mq_sessions[mq_info].create_channel
      end
    rescue Exception => e
      log e
      sleep(5)
      retry
    end
  end

  def publishToMQ(mq_info, message, args = {})
    begin
      ch = getMQChannel(mq_info)
      q  = ch.queue(mq_info[:name], mq_info[:options] == nil ? {} : mq_info[:options])
      q.publish(toJson(message), args)
      log("message published to #{mq_info}")
    rescue Exception => e
      log e
      sleep(5)
      retry
    end
  end

  def makeProcessResult(user_id, mq_info, data = nil, flag = '')
    return {:user_id=>user_id, :queue=>mq_info[:name], :data=>data, :flag=>flag}.to_json
  end

  def subscribeMQ(mq_info)
    log("start subscribe #{mq_info}")
    begin
      ch = getMQChannel(mq_info)
      q = ch.queue(mq_info[:name], mq_info[:options])
      q.subscribe(:block => true) do |delivery_info, properties, body|
        begin
          log(delivery_info)
          processMQMessage(mq_info, delivery_info, properties, body)
          log('message consumed')
        rescue Exception => e
          log(e)
        end
      end
    rescue Exception=>e
      log(e)
      sleep(5)
      retry
    end
  end

  def toJson(val)
    if(val.respond_to? :to_json)
      return val.to_json
    else
      return val.to_s
    end
  end

  def processMQMessage(mq_info, delivery_info, properties, body)
  end

  def clear
    @mq_channels.each do |mq_info, channel|
      log("close channel in #{mq_info}")
      channel.close
    end

    @mq_sessions.each do |mq_info, session|
      log("close session on #{mq_info}")
      session.close
    end

    @sql_clients.each do |db_info, client|
      log("close connection on #{db_info}")
      client.close
    end

    @cql_clients.each do |db_info, client|
      log("close connection on #{db_info}")
      client.close
    end
  end
end
