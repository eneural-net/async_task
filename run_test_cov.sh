#!/bin/bash

dart run test_cov

genhtml -o coverage coverage/lcov.info

open coverage/index.html
