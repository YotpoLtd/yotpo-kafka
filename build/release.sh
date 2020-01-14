#!/usr/bin/env bash
set -e
gem install bundler -v 1.17.1
bundle config yotpo.jfrog.io $JFROG_USER:$JFROG_PASSWORD
bundle install
bundle exec rake build
docker-compose -f ./docker-compose/docker-compose.yml up -d
sleep 7
rspec

export RUBYGEMS_HOST=https://yotpo.jfrog.io/yotpo/api/gems/gem-local
curl -u$JFROG_USER:$JFROG_PASSWORD $RUBYGEMS_HOST/api/v1/api_key.yaml > ~/.gem/credentials
chmod 0600 ~/.gem/credentials

if [[ $TRAVIS_BRANCH == "master" ]]; then
  echo "Tagging the release branch"
  git config user.email "travis-ci@yotpo.com"
  git config user.name "Travis CI"
  git tag -a "v-${BUILD_DATE}" -m "Committed by Travis-CI ${TRAVIS_BUILD_NUMBER} [ci skip]"
  git push --tags

  GEM_FILE=$(find ./pkg -name *.gem)
    if [ -z "$GEM_FILE" ]; then
        echo "Didn't find any suitable gem to release."
        echo "Packaged gems:"
        find ./ -name *.gem
        echo "Please check your release version (is your pre/release configuration correct?)."
        exit 0
    fi
  gem push $GEM_FILE
fi
