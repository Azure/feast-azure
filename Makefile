# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

# Python SDK

compile-go-lib: install-go-proto-dependencies install-go-ci-dependencies
	COMPILE_GO=True python feast/setup.py build_ext --inplace

install-python-ci-dependencies: install-go-proto-dependencies install-go-ci-dependencies
	python -m piptools sync sdk/python/requirements/py$(PYTHON)-ci-requirements.txt
	COMPILE_GO=True python setup.py develop

# Go SDK & embedded

install-go-proto-dependencies:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.26.0
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0

install-go-ci-dependencies:
	# ToDo: currently gopy installation doesn't work w/o explicit go get in the next line
	# ToDo: there should be a better way to install gopy
	go get github.com/go-python/gopy
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/go-python/gopy@latest
	python -m pip install pybindgen==0.22.0