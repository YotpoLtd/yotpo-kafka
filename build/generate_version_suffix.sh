#!/bin/bash
set -e

if [[ $TRAVIS_BRANCH == "master" ]]; then
  exit 0
fi

function generateVersion() {
  VERSION='.'
  if [[ $TRAVIS ]]; then
    if [[ -n $TRAVIS_BRANCH ]]; then
      if [[ $TRAVIS_BRANCH != "master" ]]; then
          VERSION="${VERSION}pre."
      fi
      VERSION="${VERSION}${TRAVIS_BRANCH}."
    fi
  else
    VERSION="${VERSION}localbuild."
  fi
  VERSION="${VERSION}`date +%Y.%m.%d.%H.%M.%S`"
  VERSION=${VERSION//-/.}
  VERSION=${VERSION//_/.}
  echo $VERSION
}

generateVersion
