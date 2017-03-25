package storage

import (
	"bytes"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"

	"github.com/satori/uuid"
)

type TableBatchError struct {
	Code    string
	Message string
}

func (e TableBatchError) Error() string {
	return fmt.Sprintf("Error code %s : Msg %s", e.Code, e.Message)
}

// TableBatch stores all the enties that will be operated on during a batch process.
// Entities can be inserted, replaced or deleted.
type TableBatch struct {
	InsertEntitySlice          []TableEntity
	InsertOrMergeEntitySlice   []TableEntity
	InsertOrReplaceEntitySlice []TableEntity
	ReplaceEntitySlice         []TableEntity
	MergeEntitySlice           []TableEntity
	DeleteEntitySlice          []TableEntity
}

// InsertEntity adds an entity in preparation for a batch insert.
func (b *TableBatch) InsertEntity(entity TableEntity) {
	b.InsertEntitySlice = append(b.InsertEntitySlice, entity)
}

// InsertOrReplaceEntity adds an entity in preparation for a batch insert or replace.
func (b *TableBatch) InsertOrReplaceEntity(entity TableEntity) {
	b.InsertOrReplaceEntitySlice = append(b.InsertOrReplaceEntitySlice, entity)
}

// InsertOrMergeEntity adds an entity in preparation for a batch insert or merge.
func (b *TableBatch) InsertOrMergeEntity(entity TableEntity) {
	b.InsertOrMergeEntitySlice = append(b.InsertOrMergeEntitySlice, entity)
}

// ReplaceEntity adds an entity in preparation for a batch replace.
func (b *TableBatch) ReplaceEntity(entity TableEntity) {
	b.ReplaceEntitySlice = append(b.ReplaceEntitySlice, entity)
}

// DeleteEntity adds an entity in preparation for a batch delete
func (b *TableBatch) DeleteEntity(entity TableEntity) {
	b.DeleteEntitySlice = append(b.DeleteEntitySlice, entity)
}

// MergeEntity adds an entity in preparation for a batch merge
func (b *TableBatch) MergeEntity(entity TableEntity) {
	b.MergeEntitySlice = append(b.MergeEntitySlice, entity)
}

// ExecuteBatch executes many table operations in one request to Azure.
// The operations can be combinations of Insert, Delete, Replace and Merge
//
// Creates the inner changeset body (various operations, Insert, Delete etc) then creates the outer request packet that encompasses
// the changesets.
func (c *TableServiceClient) ExecuteBatch(table AzureTable, batch *TableBatch) error {

	changesetBoundary := fmt.Sprintf("changeset_%s", uuid.NewV1())
	uri := c.client.getEndpoint(tableServiceName, "$batch", nil)
	changesetBody, err := c.generateChangesetBody(table, batch, uri, changesetBoundary)
	if err != nil {
		return err
	}

	boundary := fmt.Sprintf("batch_%s", uuid.NewV1())
	body, err := generateBody(changesetBody, uri, changesetBoundary, boundary)
	if err != nil {
		return err
	}

	headers := c.client.getStandardHeaders()
	headers[headerContentType] = fmt.Sprintf("multipart/mixed; boundary=%s", boundary)

	resp, err := c.client.execInternalJSON(http.MethodPost, uri, headers, bytes.NewReader(body.Bytes()), c.auth, true)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	if err = checkRespCode(resp.statusCode, []int{http.StatusAccepted}); err != nil {
		detailedErr := TableBatchError{}
		detailedErr.Code = resp.odata.Err.Code
		detailedErr.Message = resp.odata.Err.Message.Value
		return detailedErr
	}

	return nil
}

// generateBody generates the complete body for the batch request.
func generateBody(changeSetBody *bytes.Buffer, tableURL string, changesetBoundary string, boundary string) (*bytes.Buffer, error) {

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	writer.SetBoundary(boundary)
	h := make(textproto.MIMEHeader)
	h.Set(headerContentType, fmt.Sprintf("multipart/mixed; boundary=%s\r\n", changesetBoundary))
	batchWriter, _ := writer.CreatePart(h)
	batchWriter.Write(changeSetBody.Bytes())
	writer.Close()
	return body, nil
}

// generateChangesetBody generates the individual changesets for the various operations within the batch request.
// There is a changeset for Insert, Delete, Merge etc.
func (c *TableServiceClient) generateChangesetBody(table AzureTable, batch *TableBatch, tableURL string, changesetBoundary string) (*bytes.Buffer, error) {

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	writer.SetBoundary(changesetBoundary)

	c.generateEntitySubset(table, batch.InsertEntitySlice, insertOp, changesetBoundary, writer)
	c.generateEntitySubset(table, batch.MergeEntitySlice, mergeOp, changesetBoundary, writer)
	c.generateEntitySubset(table, batch.ReplaceEntitySlice, replaceOp, changesetBoundary, writer)
	c.generateEntitySubset(table, batch.DeleteEntitySlice, deleteOp, changesetBoundary, writer)
	c.generateEntitySubset(table, batch.InsertOrReplaceEntitySlice, insertOrReplaceOp, changesetBoundary, writer)
	c.generateEntitySubset(table, batch.InsertOrMergeEntitySlice, insertOrMergeOp, changesetBoundary, writer)
	writer.Close()
	return body, nil
}

// generateVerb generates the HTTP request VERB required for each changeset.
func generateVerb(op int) (string, error) {
	switch op {
	case insertOp:
		return "POST", nil
	case deleteOp:
		return "DELETE", nil
	case mergeOp:
		return "MERGE", nil
	case replaceOp:
		return "PUT", nil
	case insertOrReplaceOp:
		return "PUT", nil
	case insertOrMergeOp:
		return "MERGE", nil
	default:
		return "", errors.New("Unable to detect operation")
	}
}

// generateQueryPath generates the query path for within the changesets
// For inserts it will just be a table query path (table name)
// but for other operations (modifying an existing entity) then
// the partition/row keys need to be generated.s
func generateQueryPath(op int, entity TableEntity, table AzureTable) string {
	pathForTable := pathForTable(table)
	if op == insertOp {
		return pathForTable
	}
	pathForTable += fmt.Sprintf("(PartitionKey='%s',RowKey='%s')", url.QueryEscape(entity.PartitionKey()), url.QueryEscape(entity.RowKey()))
	return pathForTable
}

// generateGenericOperationHeaders generates common headers for a given operation.
// TODO(kpfaulkner) keep these as Sprintf methods of just hardcode it outright?
func generateGenericOperationHeaders(op int) []string {

	headers := []string{}
	headers = append(headers, fmt.Sprintf("%s: %s\r\n", "Accept", "application/json;odata=minimalmetadata"))
	headers = append(headers, fmt.Sprintf("%s: %s\r\n", "Content-Type", "application/json"))
	headers = append(headers, fmt.Sprintf("%s: %s\r\n", "Prefer", "return-no-content"))

	switch op {
	case deleteOp:
		headers = append(headers, fmt.Sprintf("%s: %s\r\n", "If-Match", "*"))
	}

	headers = append(headers, "\r\n")
	return headers
}

// generate body payload for particular batch operation.
func (c *TableServiceClient) generateEntitySubset(table AzureTable, entities []TableEntity, op int, boundary string, writer *multipart.Writer) error {

	h := make(textproto.MIMEHeader)
	h.Set(headerContentType, "application/http")
	h.Set(headerContentTransferEncoding, "binary")

	verb, err := generateVerb(op)
	if err != nil {
		return err
	}

	genericOpHeaders := generateGenericOperationHeaders(op)
	for _, entity := range entities {
		queryPath := generateQueryPath(op, entity, table)
		uri := c.client.getEndpoint(tableServiceName, queryPath, nil)

		operationWriter, _ := writer.CreatePart(h)
		writer.SetBoundary(boundary)

		urlAndVerb := fmt.Sprintf("%s %s HTTP/1.1\r\n", verb, uri)
		operationWriter.Write([]byte(urlAndVerb))
		for _, header := range genericOpHeaders {
			operationWriter.Write([]byte(header))
		}

		// delete operation doesn't need a body.
		if op != deleteOp {
			entityJSON, err := generateEntityJSON(entity)
			if err != nil {
				return err
			}
			operationWriter.Write(entityJSON)
		}
	}

	return nil
}

func generateEntityJSON(entity TableEntity) ([]byte, error) {

	var buf bytes.Buffer
	if err := injectPartitionAndRowKeys(entity, &buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
