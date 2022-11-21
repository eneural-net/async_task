#!/bin/bash

dart run test --coverage=./coverage

dart pub global run coverage:format_coverage --packages=.dart_tool/package_config.json --report-on=lib --lcov -o ./coverage/lcov.info -i ./coverage

genhtml -o ./coverage/report ./coverage/lcov.info
open ./coverage/report/index.html
