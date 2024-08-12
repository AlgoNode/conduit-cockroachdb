# Nodely Turbo Import Plugin

This plugin imports block and delta data from Nodely instant sync cloud follower. 

## Features

### Follower-less sourcing from Nodely cloud

* skips wait-for-round-after for rounds that are known to exists on source node
* downloads block and delta in parallel 

## Configuration
```yml @sample.yaml
importer:
    name: ndlycloud
    config:
        netaddr: "http://mainnet-api.algonode.network"
        token: ""
```
