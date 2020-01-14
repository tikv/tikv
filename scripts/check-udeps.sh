#!/bin/bash
#
# This script makes sure that there is unused dependencies in the tikv
# dependencies tree. It is suitable to run as part of CI.

cargo install cargo-udeps && cargo udeps
