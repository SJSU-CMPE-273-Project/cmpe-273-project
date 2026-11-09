# Two stages: compilers are needed to build gems with C extensions (puma),
# but not to run them. Leaving them out of the final image keeps it small and
# removes tools an attacker could use (OWASP: minimal attack surface).

# ---- build ---------------------------------------------------------------
FROM ruby:3.3-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Gemfile Gemfile.lock ./
RUN bundle config set --local without test && bundle install

# ---- test (docker build --target test .) ---------------------------------
FROM build AS test
RUN bundle config unset --local without && bundle install
COPY . .
CMD ["sh", "-c", "for f in test/*_test.rb; do ruby -Ilib \"$f\" || exit 1; done"]

# ---- runtime -------------------------------------------------------------
FROM ruby:3.3-slim
WORKDIR /app
COPY --from=build /usr/local/bundle /usr/local/bundle
COPY Gemfile Gemfile.lock config.ru app.rb ./
COPY lib ./lib

# Run as a non-root user (least privilege). The app writes nothing to disk;
# logs go to stdout, so it is stateless / ephemeral-storage only.
# Files are owned by root and only made readable, so the app can't modify its own code.
RUN useradd --system --uid 10001 app && chmod -R a+rX /app
USER app

ENV RACK_ENV=production PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD ruby -rnet/http -e 'exit Net::HTTP.get_response(URI("http://127.0.0.1:8080/health")).is_a?(Net::HTTPSuccess) ? 0 : 1'

CMD ["bundle", "exec", "puma", "-b", "tcp://0.0.0.0:8080", "-t", "8:16", "config.ru"]
