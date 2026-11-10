# Iggy has no official Ruby SDK: use the server-0.9.0 HTTP API directly.
require 'json'
require 'base64'
require 'net/http'
require 'relayguard/v1/notification_pb'

message = Relayguard::V1::NotificationRequested.decode_json(STDIN.read)
raise 'sequence outside BIGINT range' unless (1..9223372036854775807).cover?(message.sequence)
payload = Relayguard::V1::NotificationRequested.encode_json(message)
if ARGV.include?('--publish')
  base = ENV.fetch('IGGY_HTTP_URL', 'http://localhost:3000')
  call = lambda do |path, doc, token=nil|
    uri = URI(base + path)
    request = Net::HTTP::Post.new(uri)
    request['Content-Type'] = 'application/json'
    request['Authorization'] = "Bearer #{token}" if token
    request.body = JSON.generate(doc)
    response = Net::HTTP.start(uri.host, uri.port, read_timeout: 10) { |http| http.request(request) }
    raise "Iggy HTTP #{response.code}" unless response.is_a?(Net::HTTPSuccess)
    response.body.empty? ? nil : JSON.parse(response.body)
  end
  login = call.call('/users/login', {username: ENV.fetch('IGGY_ROOT_USERNAME', 'iggy'),
                                    password: ENV.fetch('IGGY_ROOT_PASSWORD', 'Secret123')})
  # Partition IDs use a base64-encoded uint32 little-endian value in HTTP.
  call.call('/streams/relayguard/topics/requests/messages', {
    partitioning: {kind: 'partition_id', value: Base64.strict_encode64([Integer(ENV.fetch('IGGY_PARTITION'))].pack('V'))},
    messages: [{id: '0', payload: Base64.strict_encode64(payload)}]
  }, login.fetch('access_token').fetch('token'))
else
  puts JSON.generate({json: JSON.parse(payload), binary: Base64.strict_encode64(Relayguard::V1::NotificationRequested.encode(message))})
end
