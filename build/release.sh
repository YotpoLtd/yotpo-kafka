#!/usr/bin/env bash
set -e
gem install bundler -v 1.17.1
bundle config yotpo.jfrog.io $JFROG_USER:$JFROG_PASSWORD
bundle install
export GEM_VERSION_SUFFIX=$(./build/generate_version_suffix.sh)
echo GEM_VERSION_SUFFIX: $GEM_VERSION_SUFFIX
bundle exec rake build
docker-compose -f ./docker-compose/docker-compose.yml up -d
sleep 7
rspec

export RUBYGEMS_HOST=https://yotpo.jfrog.io/yotpo/api/gems/gem-local
curl -u$JFROG_USER:$JFROG_PASSWORD $RUBYGEMS_HOST/api/v1/api_key.yaml > ~/.gem/credentials
chmod 0600 ~/.gem/credentials

GEM_FILE=$(find ./pkg -name *.gem)
if [ -z "$GEM_FILE" ]; then
  echo "Didn't find any suitable gem to release."
  echo "Packaged gems:"
  find ./ -name *.gem
  echo "Please check your release version (is your pre/release configuration correct?)."
  exit 0
fi

echo GEM_FILE: $GEM_FILE

if [[ $TRAVIS_BRANCH == "master" ]]; then
  gem push $GEM_FILE
fi
