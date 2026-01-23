#!/bin/bash

APIKEY=$1
shift  # remove the first argument (API key) from "$@"

## dart pub global activate dart_bump

dart_bump . \
  --api-key $APIKEY \
  "$@"
