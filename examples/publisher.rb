require 'rubygems'
require 'stomp'

#client = Stomp::Client.new("", "", "localhost", 61613)

client = Stomp::Client.new("failover://(stomp://:@localhost:61613,stomp://:@remotehost:61613)?initialReconnectDelay=5000&randomize=false&useExponentialBackOff=false")
message = "ronaldo #{ARGV[0]}"

for i in (1..300)
  puts "Sending message"
  client.send("/queue/ronaldo", "#{i}: #{message}", {:persistent => true})
  puts "(#{Time.now})Message sent: #{i}"
  sleep 1
end



