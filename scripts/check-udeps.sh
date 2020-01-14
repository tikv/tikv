#!/bin/bash
#
# This script makes sure that there is unused dependencies in the tikv
# dependencies tree. It is suitable to run as part of CI.

which cargo-udeps &>/dev/null || cargo install cargo-udeps && cargo udeps
