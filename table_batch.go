package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"

	"github.com/satori/uuid"
)

// consts for batch operations.
const (
	insertOp = 1 << iota
	deleteOp
	replaceOp
	mergeOp
	insertOrReplaceOp
	insertOrMergeOp
)

// TableBatch stores all the entities that will be operated on during a batch process.
// Entities can be inserted, replaced or deleted.
type TableBatch struct {
	InsertEntitySlice          []Entity
	InsertOrMergeEntitySlice   []Entity
	InsertOrReplaceEntitySlice []Entity
	ReplaceEntitySlice         []Entity
	MergeEntitySlice           []Entity
	DeleteEntitySlice          []Entity

	// reference to table we're operating on.
	Table *Table
}

// defaultChangesetHeaders for changeSets
var defaultChangesetHeaders = map[string]string{
	"Accept":       "application/json;odata=minimalmetadata",
	"Content-Type": "application/json",
	"Prefer":       "return-no-content",
}

// NewBatch return new TableBatch for populating.
func (t *Table) NewBatch() TableBatch {
	return TableBatch{
		Table: t,
	}
}

// InsertEntity adds an entity in preparation for a batch insert.
func (t *TableBatch) InsertEntity(entity Entity) {
	t.InsertEntitySlice = append(t.InsertEntitySlice, entity)
}

// InsertOrReplaceEntity adds an entity in preparation for a batch insert or replace.
func (t *TableBatch) InsertOrReplaceEntity(entity Entity) {
	t.InsertOrReplaceEntitySlice = append(t.InsertOrReplaceEntitySlice, entity)
}

// InsertOrMergeEntity adds an entity in preparation for a batch insert or merge.
func (t *TableBatch) InsertOrMergeEntity(entity Entity) {
	t.InsertOrMergeEntitySlice = append(t.InsertOrMergeEntitySlice, entity)
}

// ReplaceEntity adds an entity in preparation for a batch replace.
func (t *TableBatch) ReplaceEntity(entity Entity) {
	t.ReplaceEntitySlice = append(t.ReplaceEntitySlice, entity)
}

// DeleteEntity adds an entity in preparation for a batch delete
func (t *TableBatch) DeleteEntity(entity Entity) {
	t.DeleteEntitySlice = append(t.DeleteEntitySlice, entity)
}

// MergeEntity adds an entity in preparation for a batch merge
func (t *TableBatch) MergeEntity(entity Entity) {
	t.MergeEntitySlice = append(t.MergeEntitySlice, entity)
}

// ExecuteBatch executes many table operations in one request to Azure.
// The operations can be combinations of Insert, Delete, Replace and Merge
// Creates the inner changeset body (various operations, Insert, Delete etc) then creates the outer request packet that encompasses
// the changesets.
// As per document https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/performing-entity-group-transactions
func (t *TableBatch) ExecuteBatch() error {

	changesetBoundary := fmt.Sprintf("changeset_%s", uuid.NewV1())
	uri := t.Table.tsc.client.getEndpoint(tableServiceName, "$batch", nil)
	changesetBody, err := t.generateChangesetBody(changesetBoundary)
	if err != nil {
		return err
	}

	boundary := fmt.Sprintf("batch_%s", uuid.NewV1())
	body, err := generateBody(changesetBody, changesetBoundary, boundary)
	if err != nil {
		return err
	}

	headers := t.Table.tsc.client.getStandardHeaders()
	headers[headerContentType] = fmt.Sprintf("multipart/mixed; boundary=%s", boundary)

	resp, err := t.Table.tsc.client.execBatchOperationJSON(http.MethodPost, uri, headers, bytes.NewReader(body.Bytes()), t.Table.tsc.auth)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	if err = checkRespCode(resp.statusCode, []int{http.StatusAccepted}); err != nil {
		requestID, date, version := getDebugHeaders(resp.headers)

		return AzureStorageServiceError{
			StatusCode: resp.statusCode,
			Code:       resp.odata.Err.Code,
			RequestID:  requestID,
			Date:       date,
			APIVersion: version,
			Message:    resp.odata.Err.Message.Value,
		}
	}

	return nil
}

// generateBody generates the complete body for the batch request.
func generateBody(changeSetBody *bytes.Buffer, changesetBoundary string, boundary string) (*bytes.Buffer, error) {

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
func (t *TableBatch) generateChangesetBody(changesetBoundary string) (*bytes.Buffer, error) {

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	writer.SetBoundary(changesetBoundary)

	t.generateEntitySubset(insertOp, changesetBoundary, writer)
	t.generateEntitySubset(mergeOp, changesetBoundary, writer)
	t.generateEntitySubset(replaceOp, changesetBoundary, writer)
	t.generateEntitySubset(deleteOp, changesetBoundary, writer)
	t.generateEntitySubset(insertOrReplaceOp, changesetBoundary, writer)
	t.generateEntitySubset(insertOrMergeOp, changesetBoundary, writer)
	writer.Close()
	return body, nil
}

// generateVerb generates the HTTP request VERB required for each changeset.
func generateVerb(op int) (string, error) {
	switch op {
	case insertOp:
		return http.MethodPost, nil
	case deleteOp:
		return http.MethodDelete, nil
	case replaceOp, insertOrReplaceOp:
		return http.MethodPut, nil
	case mergeOp, insertOrMergeOp:
		return "MERGE", nil
	default:
		return "", errors.New("Unable to detect operation")
	}
}

// generateQueryPath generates the query path for within the changesets
// For inserts it will just be a table query path (table name)
// but for other operations (modifying an existing entity) then
// the partition/row keys need to be generated.
func (t *TableBatch) generateQueryPath(op int, entity Entity) string {
	if op == insertOp {
		return entity.Table.buildPath()
	}
	return entity.buildPath()
}

// generateGenericOperationHeaders generates common headers for a given operation.
func generateGenericOperationHeaders(op int, force bool, e *Entity) map[string]string {
	retval := map[string]string{}

	for k, v := range defaultChangesetHeaders {
		retval[k] = v
	}

	if op == deleteOp || op == replaceOp || op == mergeOp {
		if force {
			retval["If-Match"] = "*"
		} else {
			retval["If-Match"] = e.OdataEtag
		}
	}

	return retval
}

func (t *TableBatch) getEntitiesForOperation(op int) ([]Entity, error) {
	switch op {
	case insertOp:
		return t.InsertEntitySlice, nil
	case deleteOp:
		return t.DeleteEntitySlice, nil
	case mergeOp:
		return t.MergeEntitySlice, nil
	case replaceOp:
		return t.ReplaceEntitySlice, nil
	case insertOrReplaceOp:
		return t.InsertOrReplaceEntitySlice, nil
	case insertOrMergeOp:
		return t.InsertOrMergeEntitySlice, nil
	default:
		return nil, errors.New("Unable to detect operation")
	}
}

// generateEntitySubset generates body payload for particular batch operation.
func (t *TableBatch) generateEntitySubset(op int, boundary string, writer *multipart.Writer) error {

	entities, err := t.getEntitiesForOperation(op)
	if err != nil {
		return err
	}

	h := make(textproto.MIMEHeader)
	h.Set(headerContentType, "application/http")
	h.Set(headerContentTransferEncoding, "binary")

	verb, err := generateVerb(op)
	if err != nil {
		return err
	}

	for _, entity := range entities {
		genericOpHeadersMap := generateGenericOperationHeaders(op, true, &entity)
		queryPath := t.generateQueryPath(op, entity)
		uri := t.Table.tsc.client.getEndpoint(tableServiceName, queryPath, nil)

		operationWriter, _ := writer.CreatePart(h)
		writer.SetBoundary(boundary)

		urlAndVerb := fmt.Sprintf("%s %s HTTP/1.1\r\n", verb, uri)
		operationWriter.Write([]byte(urlAndVerb))
		for k, v := range genericOpHeadersMap {
			operationWriter.Write([]byte(fmt.Sprintf("%s: %s\r\n", k, v)))
		}
		operationWriter.Write([]byte("\r\n")) // additional \r\n is needed per changeset separating the "headers" and the body.

		// delete operation doesn't need a body.
		if op != deleteOp {
			body, err := json.Marshal(entity.Properties)
			if err != nil {
				return err
			}
			operationWriter.Write(body)
		}
	}

	return nil
}
