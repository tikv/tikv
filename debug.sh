#!/bin/bash

set -e

export CARGO_PROFILE_DEV_DEBUG="true"
export CARGO_PROFILE_RELEASE_DEBUG="true"

make build