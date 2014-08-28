require 'zk'
require 'yaml'
require 'erb'
require 'resolv'
require 'set'
require 'fileutils'
require 'uri'
require 'cql'
require 'cql/compression/snappy_compressor'

module Cassandra
  EXIT_SUCCESS = 0
  class Server
    def initialize(heap_size, server_count, ports, cassandra, config)
      @heap_size = heap_size.to_i
      @prng = Random.new(Time.now.to_f * 100000)
      @cassandra = cassandra

      @yaml = YAML.load_file(config)

      @zk_servers = @yaml['zk_hosts'].shuffle.join(',')
      @cluster = @yaml['cluster']
      @s3cmd_config = @yaml['s3cmd_config']

      zk_connect

      @ports = ports.to_s.split(/,/)

      @server_count = server_count.to_i

      @server_set = get_missing_servers
      if @server_set.empty?
        $stderr.puts "No missing servers found!"
        $stdout.puts "No missing servers found!"
        java.lang.System.exit(1)
      end

      @max_attempts = 3 * @server_count

      @hostname = `hostname`.chomp
      @ip = '127.0.0.1'

      Resolv::DNS.open do |dns|
        ress = dns.getresources @hostname, Resolv::DNS::Resource::IN::A
        @ip = ress.map { |r| r.address }.first.to_s
      end

      # initial seed is itself, if there are no other instances running
      @seeds = Set.new [@ip]

      @snapshot_exclusions = %w{
        local
        compactions_in_progress
        compaction_history
        NodeIdInfo
        hints
        peers
        peer_events
      }
    end

    def zk_connect
      @zk = ZK.new(@zk_servers, {
        :chroot    => "/cassandra-#{@cluster}",
        :thread    => :single,
        :timeout   => 5,
      })

      @zk.wait_until_connected
    end

    def zk_reconnect
      close
      sleep 10
      zk_connect
      become_server
    end

    # Initially, populate server set with list of servers not in Zookeeper, if
    # possible
    def get_missing_servers
      server_set = Set.new(0..@server_count - 1)
      if @zk.exists?('/servers/ids') && @zk.stat('/servers/ids').num_children > 0
        ids = @zk.children('/servers/ids').map{ |x| x.to_i }.sort
        puts "Found these server IDs in Zookeeper: #{ids}"
        server_set = server_set.subtract(ids)
      end
      puts "Missing server IDs: #{server_set.to_a.sort}"
      server_set
    end

    def get_seeds
      new_seeds = Set.new
      begin
        if @zk.exists?('/servers/ids') && @zk.stat('/servers/ids').num_children > 0
          ids = @zk.children('/servers/ids').map{ |x| x.to_i }.sort
          puts "Found these server IDs in Zookeeper: #{ids}"
          ids.each do |id|
            next if id == @server_id # skip this server
            data = @zk.get("/servers/ids/#{id}").first.to_s
            if data == @ip
              $stderr.puts "There's already an instance of C* running at address #{ip}!"
              $stdout.puts
              java.lang.System.exit(1)
            end
            new_seeds.add(data)
          end
        end
      rescue => e
        $stderr.puts $!.inspect, $@
      end
      if new_seeds.size > 0
        @seeds = new_seeds
        @seeds.add(@ip)
      end
    end

    def close
      @node_subscription.unsubscribe unless @node_subscription.nil?
      @zk.delete(@server_key) rescue nil
      @candidate.close unless @candidate.nil?
      @zk.close
    end

    def run_command(command, kill_after=3600)
      puts "Executing command: '#{command.join(' ')}'"
      cmd = ["timeout", kill_after.to_s] + command
      system(*cmd)
      status = $?.exitstatus
      if status != EXIT_SUCCESS
        puts "Command '#{command.join(' ')}' return non-zero status #{status}"
      end
      return status == EXIT_SUCCESS
    end

    def become_server
      elected = false
      got_result = false

      @candidate = @zk.election_candidate(
        "cassandra-#{@server_id}", @hostname, :follow => :leader)

      @candidate.on_winning_election {
        puts "Won election for cassandra-#{@server_id}"
        elected = true
        got_result = true
        @server_key = "/servers/ids/#{@server_id}"

        if @zk.exists?(@server_key)
          @zk.delete(@server_key)
        end
        @zk.mkdir_p('/servers/ids')
        @zk.create(@server_key, @ip, :mode => :ephemeral)

        @node_subscription = @zk.register(@server_key, :only => [:changed, :deleted]) do |event|
          puts "ZK node subscription event received", event
          zk_reconnect
        end

        unless @zk.exists?(@server_key, :watch => true)
          puts "ZK node subscription already exists"
          zk_reconnect
        end

        @zk.on_expired_session do
          puts "ZK session expired"
          zk_reconnect
        end
      }

      @candidate.on_losing_election {
        puts "Lost election for cassandra-#{@server_id}"

        elected = false
        got_result = true
      }

      @attempts = 0

      while !got_result
        puts "Trying to get elected for cassandra-#{@server_id}..."
        @candidate.vote!
        # Random sleep to help avoid thundering herd
        sleep @prng.rand(@server_count)
        @attempts += 1
        if @attempts > @max_attempts
          break
        end
      end

      if !elected
        close
        $stderr.puts "Couldn't become a server. Suiciding."
        $stdout.puts "Couldn't become a server. Suiciding."
        java.lang.System.exit(1)
      end
    end

    def create_snapshot
      puts "Creating snapshot"
      snapshot = `timeout 60 ./#{@cassandra}/bin/nodetool -h localhost snapshot`
      puts "Finished snapshotting"
      m = /Snapshot directory: (\d+)/.match snapshot
      if m
        snapshot = m[1]
        puts "Creating archive for snapshot #{snapshot}"
        excludes = @snapshot_exclusions.map{|p| "-not -path 'cassandra/data/system/#{p}/*'"}.join(' ')
        system("find cassandra -type d -iname '#{snapshot}' #{excludes} > snapshot-files")
        %x{export XZ_OPT=-2v ; tar --totals -cJf snapshot.tar.xz -T snapshot-files}
        puts "Uploading snapshot.tar.xz (#{File.size('snapshot.tar.xz')} bytes)"
        run_command ['s3cmd', '-c', @s3cmd_config, 'put', 'snapshot.tar.xz', @snapshot_uri]
      else
        $stderr.puts "Failed to create snapshot!  Nodetool ended with status #{$?.exitstatus} and printed: #{snapshot}"
      end
      # clean up old snapshots
      run_command ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'clearsnapshot'], 60
    end

    def cconnect
      compressor = Cql::Compression::SnappyCompressor.new
      Cql::Client.connect(
        hosts: [@ip],
        port: @ports[0],
        keyspace: 'system',
        compressor: compressor
      )
    end

    def run
      @server_id = @server_set.to_a.sample(:random => @prng)
      become_server

      @snapshot_uri = URI.join(
        @yaml['snapshot_uri'],
        'marathon',
        'cassandra',
        'snapshots',
        @cluster,
        @server_id
        'snapshot.tar.xz'
      )

      at_exit {
        close
      }

      env = {
        "MAX_HEAP_SIZE" => "#{@heap_size.to_s}m",
        "HEAP_NEWSIZE" => "#{(@heap_size / 3).to_s}m",
        "CASSANDRA_CONF" => ".",
      }

      run_command ['tar', 'xf', "#{@cassandra}-bin.tar.xz"]

      # Initialize cassandra from snapshots, if available
      if run_command ['s3cmd', '-c', @s3cmd_config, 'get', @snapshot_uri]
        puts "About to initialize from snapshot"
        run_command %w{tar --totals -xf snapshot.tar.xz}
        Dir.chdir('cassandra') do
          Dir.glob('**/snapshots').each do |dir|
            Dir.chdir(dir) do
              # exclude some parts of the system keyspace
              next if @snapshot_exclusions.find {|p| dir =~ /^data\/system\/#{p}\// }
              # find the most recent snapshot
              cur = Dir.glob('*').sort{|a,b|File.mtime(b)<=>File.mtime(a)}.max
              next unless File.directory?(cur.to_s)
              Dir.chdir(cur.to_s) do
                Dir.glob('*').each do |fn|
                  if File.file?(fn) # is a regular file?
                    puts "Moving #{fn} to #{File.expand_path('../..')}"
                    FileUtils.mv(fn, '../..')
                  end
                end
              end
            end
          end
        end
      end
      FileUtils.rm('snapshot.tar.xz')

      # Launch backup loop
      Thread.new do
        sleep 60
        # Run the initial nodetool repair & scrub
        puts "Beginning keyspace repair"
        run_command ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'repair', '-par'], 8*3600 # 8 hour timeout
        puts "Finished repair"
        puts "Beginning keyspace scrub"
        run_command ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'scrub'], 8*3600 # 8 hour timeout
        puts "Finished scrub"
        sleep_time = 3600 * 18 # 18 hours
        splay = 3600 * 3 # 3 hours
        sleep 300 + @prng.rand(splay) # sleep for at least 5 minutes, then do an initial backup
        ran_repair = false # run the repair once more initially
        loop do
          # only run the repair every other time
          if !ran_repair
            keyspaces = []
            client = nil
            begin
              client = cconnect
              s = client.prepare 'SELECT keyspace_name FROM system.schema_keyspaces'
              res = s.execute :one
              if !res.empty?
                res.each do |x|
                  keyspaces << x['keyspace_name']
                end
              end
            rescue => e
              $stderr.puts $!.inspect, $@
            ensure
              client.close unless client.nil?
            end

            keyspaces.each do |keyspace|
              puts "Beginning keyspace repair for #{keyspace}"
              # occasionally, do a full repair
              full_repair = @prng.rand(@server_count * 2) == 0
              Thread.new do
                if full_repair
                  begin
                    @zk.with_lock("#{keyspace}-repair-lock", :wait => 1200) do
                      puts "Acquired ZK lock"
                      run_command(
                        ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'repair', '-par', keyspace],
                        8*3600 # 8 hour timeout
                      )
                    end
                  rescue ZK::Exceptions::LockWaitTimeoutError
                    $stderr.puts $!.inspect, $@
                  end
                else
                  puts "No ZK lock acquired (not necessary)"
                  run_command(
                    ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'repair', '-par', '-pr', keyspace],
                    8*3600 # 8 hour timeout
                  )
                end
                puts "Finished keyspace repair for #{keyspace}"
              end
              ran_repair = true

              puts "Beginning keyspace compact for #{keyspace}"
              begin
                @zk.with_lock("#{keyspace}-compact-lock", :wait => 1200) do
                  puts "Acquired ZK lock"
                  run_command ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'compact', keyspace], 8*3600 # 8 hour timeout
                end
              rescue ZK::Exceptions::LockWaitTimeoutError
                $stderr.puts $!.inspect, $@
              end
              puts "Finished keyspace compact for #{keyspace}"
              if full_repair
                puts "Beginning keyspace cleanup for #{keyspace}"
                begin
                  @zk.with_lock("#{keyspace}-cleanup-lock", :wait => 1200) do
                    puts "Acquired ZK lock"
                    run_command ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'cleanup', keyspace], 8*3600 # 8 hour timeout
                  end
                rescue ZK::Exceptions::LockWaitTimeoutError
                  $stderr.puts $!.inspect, $@
                end
                puts "Finished keyspace cleanup for #{keyspace}"
              end
            end
          else
            ran_repair = false
          end

          # create snapshot, upload to S3, remove snapshots
          create_snapshot
          sleep sleep_time + @prng.rand(-splay..splay)
        end
      end

      # Launch health check loop
      Thread.new do
        sleep 500
        @nodes_down = Set.new
        @previous_state = false
        loop do
          # check if this node has entered into a bad state
          bad = false

          get_seeds

          # look for dead nodes
          status = `timeout 60 ./#{@cassandra}/bin/nodetool -h localhost status`
          down = Set.new
          nodes_up = Set.new
          status.split(/\r?\n/).each do |line|
            match = /^D[NLJM]\s+([0-9\.]+)\s+[0-9\.\s]+\w+[0-9\.\s]+%\s+([a-zA-Z0-9-]+)\s+/.match line
            if match && !@seeds.include?(match[1])
              down.add(match[2])
            end
            match = /^UN\s+([0-9\.]+)\s+[0-9\.\s]+\w+[0-9\.\s]+%\s+[a-zA-Z0-9-]+\s+/.match line
            if match
              nodes_up.add(match[1])
            end
          end

          to_remove = down & @nodes_down

          to_remove.each do |node|
            begin
              @zk.with_lock("removenode-#{node}-lock", :wait => 60) do
                puts "Acquired ZK lock for #{node}"
                puts "Removing node #{node}"
                success = run_command ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'removenode', node], 1*3600 # 1 hour timeout
                if !success
                  sleep 5
                  run_command ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'removenode', 'force'], 60 # 60 second timeout
                end
              end
            rescue ZK::Exceptions::LockWaitTimeoutError
              $stdout.puts "Couldn't acquire ZK removenode lock for #{node} in a timely fashion, skipping"
              $stderr.puts $!.inspect, $@
            end
          end

          @nodes_down = @nodes_down | down

          found_self = false
          if nodes_up.size > 1
            other_node = (nodes_up - [@ip]).to_a.sample
            # randomly check another node to ensure this node is still part of the cluster, and is 'up'
            status = `timeout 60 ./#{@cassandra}/bin/nodetool -h #{other_node} status`
            if $?.to_i != EXIT_SUCCESS
              # ignore bad signals from nodetool status
              found_self = true
            else
              status.split(/\r?\n/).each do |line|
                match = /^UN\s+([0-9\.]+)\s+[0-9\.\s]+\w+[0-9\.\s]+%\s+[a-zA-Z0-9-]+\s+/.match line
                if match
                  if match[1] == @ip
                    found_self = true
                  end
                end
              end
            end
          else
            # no other nodes in the cluster?
            found_self = true
          end

          if nodes_up.size < @seeds.size
            $stderr.puts "nodes_up.size=#{nodes_up.size}, which is less than @seeds.size=#{@seeds.size}"
          end

          info = `./#{@cassandra}/bin/nodetool -h localhost info`
          if $?.to_i != EXIT_SUCCESS
            $stderr.puts "`nodetool info` return non-zero status: #{$?.to_i}"
            bad = true
          elsif !found_self
            $stderr.puts "this node did not appear in nodetool status"
            # temporarily disabled
            # bad = true
          else
            info_split = info.lines.map(&:chomp)
            info_split.each do |line|
              if line =~ /Thrift active\s+: false/i || line =~ /Native Transport active:\s+false/i || line =~ /Gossip active\s+: false/i
                $stderr.puts "`nodetool info` returned unhealthy status:\n"
                $stderr.puts info_split.join("\n")
                bad = true
              end
            end
          end
          if bad && @previous_state == bad
            str = 'Node has entered bad state.  Will create snapshot, decommission, and terminate.'
            puts str
            $stderr.puts str
            $stdout.puts str
            create_snapshot
            run_command ["./#{@cassandra}/bin/nodetool", '-h', 'localhost', 'decommission'], 3600 # 1 hour timeout
            java.lang.System.exit(1)
          end
          @previous_state = bad

          client = nil
          begin
            client = cconnect
            s = client.prepare 'SELECT peer FROM system.peers'
            res = s.execute :one
            peers = Set.new
            if !res.empty?
              res.each do |x|
                p = x['peer']
                if !@seeds.include? p.to_s
                  peers.add(p)
                end
              end
            end
            if !peers.empty?
              puts "About to remove these peers from system table: #{peers.map{|p|p.to_s}.to_s}"
              s = client.prepare 'DELETE FROM system.peers WHERE peer = ?'
              peers.each do |p|
                res = s.execute p, :one
              end
            end
          rescue => e
            $stderr.puts $!.inspect, $@
          ensure
            client.close unless client.nil?
          end

          sleep 1200 + @prng.rand(500)
        end
      end

      puts "Adding JNA symlink /usr/share/java/jna.jar -> ./#{@cassandra}/lib/jna.jar"
      File.symlink('/usr/share/java/jna.jar', "./#{@cassandra}/lib/jna.jar")

      cmd = "./#{@cassandra}/bin/cassandra -f -Dcassandra.metricsReporterConfigFile=metrics-reporter.yaml".freeze
      last_finished = 0

      loop do
        get_seeds

        %w{cassandra.yaml metrics-reporter.yaml}.each do |fn|
          erb = ERB.new(File.open(fn + '.erb').readlines.map{|x| x.chomp }.join("\n"))
          File.open(fn, 'w') do |f|
            f.puts erb.result(binding)
          end
        end

        FileUtils.mkdir_p('cassandra/commitlog')
        FileUtils.mkdir_p('cassandra/saved_caches')
        puts "About to run:"
        puts env, cmd
        GC.start # clean up memory
        system env, cmd
        finished = Time.now.to_f
        if finished - last_finished < 120
          # If the process was running for less than 2 minutes, abort.  We're
          # probably 'bouncing'.  Occasional restarts are okay, but not
          # continuous restarting.
          raise "Cassandra exited too soon!"
          java.lang.System.exit(1)
        end
        last_finished = finished
      end
    end
  end
end

server = nil

begin
  # Verify ulimit settings
  minimum_ofiles = 10000
  ofiles = `bash -c 'ulimit -n'`.chomp.to_i
  if ofiles < minimum_ofiles
    raise "Value of 'ulimit -n' (number of open files) is less than #{minimum_ofiles}!"
  end

  server = Cassandra::Server.new ARGV[0], ARGV[1], ARGV[2], ARGV[3], ARGV[4]
  server.run
rescue => e
  $stderr.puts $!.inspect, $@
ensure
  server.close unless server.nil?
end
