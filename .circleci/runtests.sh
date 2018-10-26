#!/usr/bin/env bash

set -e

py.test --cov=rioredis --strict -v
codecov