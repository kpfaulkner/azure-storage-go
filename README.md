# Azure Storage SDK for Go
[![GoDoc](https://godoc.org/github.com/Azure/azure-storage-go?status.svg)](https://godoc.org/github.com/Azure/azure-storage-go) [![Build Status](https://travis-ci.org/Azure/azure-storage-go.svg?branch=master)](https://travis-ci.org/Azure/azure-storage-go) [![Go Report Card](https://goreportcard.com/badge/github.com/Azure/azure-storage-go)](https://goreportcard.com/report/github.com/Azure/azure-storage-go)

The `github.com/Azure/azure-sdk-for-go/storage` package is used to perform REST operations against the [Azure Storage Service](https://docs.microsoft.com/en-us/azure/storage/). To manage your storage accounts (Azure Resource Manager / ARM), use the [github.com/Azure/azure-sdk-for-go/arm/storage](https://github.com/Azure/azure-sdk-for-go/tree/master/arm/storage) package. For your classic storage accounts (Azure Service Management / ASM), use [github.com/Azure/azure-sdk-for-go/management/storageservice](https://github.com/Azure/azure-sdk-for-go/tree/master/management/storageservice) package.

This package includes support for [Azure Storage Emulator](https://azure.microsoft.com/documentation/articles/storage-use-emulator/)

# Getting Started

 1. `go get -u github.com/Azure/azure-storage-go`
 2. Add the following import statement into any Go source file that will reference Azure Storage: `import storage github.com/Azure/azure-storage-go`
 3. If you don't already have one, [create a Storage Account](https://docs.microsoft.com/en-us/azure/storage/storage-create-storage-account).
      - Take note of your Azure Storage Account Name and Azure Storage Account Key. They'll both be necessary for using this library.
      - This option is production ready, but can also be used for development.
 4. (Optional, Windows only) Download and start the [Azure Storage Emulator](https://azure.microsoft.com/documentation/articles/storage-use-emulator/).
 5. Checkout our existing [samples](https://github.com/Azure-Samples?q=Storage&language=go).

# Contributing

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

When contributing, please conform to the following practices:
 - Target the 'dev' branch. This practice ensures that `go get` retreives more stable bits.
 - Run [gofmt](https://golang.org/cmd/gofmt/) to use standard go formatting.
 - Run [golint](https://github.com/golang/lint) to conform to standard naming conventions.
 - Run [go vet](https://golang.org/cmd/vet/) to catch common Go mistakes.
 - Use [GoASTScanner/gas](https://github.com/GoASTScanner/gas) to ensure there are no common security violations in your contribution.
