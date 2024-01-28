#!/bin/bash

#static 3MB build, no distro, no shell :)
docker build . -t urtho/conduit-cdb:latest
docker push urtho/conduit-cdb:latest
