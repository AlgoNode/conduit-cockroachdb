# Nodely Turbo Import Plugin

This plugin imports block and delta data from Nodely instant sync cloud follower.

## Features

### Follower-less sourcing from Nodely cloud
* skips wait-for-round-after for rounds that are known to exist
* downloads blocks and deltas concurrently
* downloads data for several rounds concurrently

## Configuration
```yml @sample.yaml
importer:
    name: ndlycloud
    config:
        netaddr: "http://mainnet-api.algonode.network"
        token: ""
        workers: 8
```
