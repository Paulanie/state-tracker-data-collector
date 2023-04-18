#!/usr/bin/env bash

mkdir -p azurite
azurite -l ./azurite &
exec func start