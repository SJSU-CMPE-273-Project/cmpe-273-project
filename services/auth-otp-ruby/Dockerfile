FROM ruby:3.4.6-slim
RUN gem install google-protobuf -v 4.33.5 --no-document
WORKDIR /workspace
ENV RUBYLIB=/workspace/gen/ruby
ENTRYPOINT ["ruby", "services/auth-otp-ruby/producer.rb"]
