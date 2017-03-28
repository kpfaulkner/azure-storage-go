package storage

import (
	"net/url"
	"time"

	"github.com/satori/uuid"

	chk "gopkg.in/check.v1"
)

func (s *StorageTableSuite) Test_BatchInsertMultipleEntities(c *chk.C) {
	cli := getBasicClient(c).GetTableService()
	table := cli.GetTableReference(randTable())

	err := table.Create(EmptyPayload, 30)
	c.Assert(err, chk.IsNil)
	defer table.Delete(30)

	entity := table.GetEntityReference("mypartitionkey", "myrowkey")
	props := map[string]interface{}{
		"AmountDue":      200.23,
		"CustomerCode":   uuid.FromStringOrNil("c9da6455-213d-42c9-9a79-3e9149a57833"),
		"CustomerSince":  time.Date(1992, time.December, 20, 21, 55, 0, 0, time.UTC),
		"IsActive":       true,
		"NumberOfOrders": int64(255),
	}
	entity.Properties = props

	entity2 := table.GetEntityReference("mypartitionkey", "myrowkey2")
	props2 := map[string]interface{}{
		"AmountDue":      111.23,
		"CustomerCode":   uuid.FromStringOrNil("c9da6455-213d-42c9-9a79-3e9149a57833"),
		"CustomerSince":  time.Date(1992, time.December, 20, 21, 55, 0, 0, time.UTC),
		"IsActive":       true,
		"NumberOfOrders": int64(255),
	}
	entity2.Properties = props2

	batch := table.NewTableBatch()
	batch.InsertOrReplaceEntity(entity)
	batch.InsertOrReplaceEntity(entity2)

	err = batch.ExecuteBatch()
	c.Assert(err, chk.IsNil)

	entities, err := table.ExecuteQuery(url.Values{}, 30)
	c.Assert(err, chk.IsNil)
	c.Assert(entities.Entities, chk.HasLen, 2)
}

func (s *StorageTableSuite) Test_BatchInsertSameEntryMultipleTimes(c *chk.C) {
	cli := getBasicClient(c).GetTableService()
	table := cli.GetTableReference(randTable())

	err := table.Create(EmptyPayload, 30)
	c.Assert(err, chk.IsNil)
	defer table.Delete(30)

	entity := table.GetEntityReference("mypartitionkey", "myrowkey")
	props := map[string]interface{}{
		"AmountDue":      200.23,
		"CustomerCode":   uuid.FromStringOrNil("c9da6455-213d-42c9-9a79-3e9149a57833"),
		"CustomerSince":  time.Date(1992, time.December, 20, 21, 55, 0, 0, time.UTC),
		"IsActive":       true,
		"NumberOfOrders": int64(255),
	}
	entity.Properties = props

	batch := table.NewTableBatch()
	batch.InsertOrReplaceEntity(entity)
	batch.InsertOrReplaceEntity(entity)

	err = batch.ExecuteBatch()
	c.Assert(err, chk.NotNil)
	v, ok := err.(TableBatchError)
	if ok {
		c.Assert(v.Code, chk.Equals, "InvalidDuplicateRow")
	}
}

func (s *StorageTableSuite) Test_BatchInsertDeleteSameEntity(c *chk.C) {
	cli := getBasicClient(c).GetTableService()
	table := cli.GetTableReference(randTable())

	err := table.Create(EmptyPayload, 30)
	c.Assert(err, chk.IsNil)
	defer table.Delete(30)

	entity := table.GetEntityReference("mypartitionkey", "myrowkey")
	props := map[string]interface{}{
		"AmountDue":      200.23,
		"CustomerCode":   uuid.FromStringOrNil("c9da6455-213d-42c9-9a79-3e9149a57833"),
		"CustomerSince":  time.Date(1992, time.December, 20, 21, 55, 0, 0, time.UTC),
		"IsActive":       true,
		"NumberOfOrders": int64(255),
	}
	entity.Properties = props

	batch := table.NewTableBatch()
	batch.InsertOrReplaceEntity(entity)
	batch.DeleteEntity(entity)

	err = batch.ExecuteBatch()
	c.Assert(err, chk.NotNil)

	v, ok := err.(TableBatchError)
	if ok {
		c.Assert(v.Code, chk.Equals, "InvalidDuplicateRow")
	}
}

func (s *StorageTableSuite) Test_BatchInsertThenDeleteDifferentBatches(c *chk.C) {
	cli := getBasicClient(c).GetTableService()
	table := cli.GetTableReference(randTable())

	err := table.Create(EmptyPayload, 30)
	c.Assert(err, chk.IsNil)
	defer table.Delete(30)

	entity := table.GetEntityReference("mypartitionkey", "myrowkey")
	props := map[string]interface{}{
		"AmountDue":      200.23,
		"CustomerCode":   uuid.FromStringOrNil("c9da6455-213d-42c9-9a79-3e9149a57833"),
		"CustomerSince":  time.Date(1992, time.December, 20, 21, 55, 0, 0, time.UTC),
		"IsActive":       true,
		"NumberOfOrders": int64(255),
	}
	entity.Properties = props

	batch := table.NewTableBatch()
	batch.InsertOrReplaceEntity(entity)
	err = batch.ExecuteBatch()
	c.Assert(err, chk.IsNil)

	entities, err := table.ExecuteQuery(url.Values{}, 30)
	c.Assert(err, chk.IsNil)
	c.Assert(entities.Entities, chk.HasLen, 1)

	batch = table.NewTableBatch()
	batch.DeleteEntity(entity)
	err = batch.ExecuteBatch()
	c.Assert(err, chk.IsNil)

	entities, err = table.ExecuteQuery(url.Values{}, 30)
	c.Assert(err, chk.IsNil)
	c.Assert(entities.Entities, chk.HasLen, 0)
}

func (s *StorageTableSuite) Test_BatchInsertThenMergeDifferentBatches(c *chk.C) {
	cli := getBasicClient(c).GetTableService()
	table := cli.GetTableReference(randTable())

	err := table.Create(EmptyPayload, 30)
	c.Assert(err, chk.IsNil)
	defer table.Delete(30)

	entity := table.GetEntityReference("mypartitionkey", "myrowkey")
	props := map[string]interface{}{
		"AmountDue":      200.23,
		"CustomerCode":   uuid.FromStringOrNil("c9da6455-213d-42c9-9a79-3e9149a57833"),
		"CustomerSince":  time.Date(1992, time.December, 20, 21, 55, 0, 0, time.UTC),
		"IsActive":       true,
		"NumberOfOrders": int64(255),
	}
	entity.Properties = props

	batch := table.NewTableBatch()
	batch.InsertOrReplaceEntity(entity)
	err = batch.ExecuteBatch()
	c.Assert(err, chk.IsNil)

	entity2 := table.GetEntityReference("mypartitionkey", "myrowkey")
	props2 := map[string]interface{}{
		"AmountDue":      200.23,
		"CustomerCode":   uuid.FromStringOrNil("c9da6455-213d-42c9-9a79-3e9149a57833"),
		"CustomerSince":  time.Date(1992, time.December, 20, 21, 55, 0, 0, time.UTC),
		"DifferentField": 123,
		"NumberOfOrders": int64(255),
	}
	entity2.Properties = props2

	batch = table.NewTableBatch()
	batch.InsertOrReplaceEntity(entity2)
	err = batch.ExecuteBatch()
	c.Assert(err, chk.IsNil)

	entities, err := table.ExecuteQuery(url.Values{}, 30)
	c.Assert(err, chk.IsNil)
	c.Assert(entities.Entities, chk.HasLen, 1)
}
