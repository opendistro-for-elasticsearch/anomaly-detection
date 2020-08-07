/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ad

import (
	"context"
	"encoding/json"
	"errors"
	"esad/internal/controller/ad/mocks"
	"esad/internal/entity/ad"
	"esad/internal/mapper"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func getRawFilter() []byte {
	return []byte(`{
    "bool": {
      "filter": {
        "term": {
          "currency": "EUR"
        }
    }}
  }`)
}

func getCreateDetectorRequest() ad.CreateDetectorRequest {
	return ad.CreateDetectorRequest{
		Name:        "test-detector-ecommerce0",
		Description: "Test detector",
		TimeField:   "utc_time",
		Index:       []string{"kibana_sample_data_ecommerce*"},
		Features: []ad.FeatureRequest{{
			AggregationType: []string{"sum", "average"},
			Enabled:         true,
			Field:           []string{"total_quantity"},
		}},
		Filter:         getRawFilter(),
		Interval:       "1m",
		Delay:          "1m",
		Start:          true,
		PartitionField: mapper.StringToStringPtr("day_of_week"),
	}
}
func TestHandler_CreateAnomalyDetector(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	t.Run("test create success", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().CreateMultiEntityAnomalyDetector(ctx, getCreateDetectorRequest(), false, true).Return([]string{"test-detector-ecommerce0-one"}, nil)
		instance := New(mockedController)
		err := CreateAnomalyDetector(instance, "testdata/create.json", false)
		assert.NoError(t, err)
	})
	t.Run("test create failure", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().CreateMultiEntityAnomalyDetector(ctx, getCreateDetectorRequest(), false, true).Return(nil, errors.New("failed to create"))
		instance := New(mockedController)
		err := CreateAnomalyDetector(instance, "testdata/create.json", false)
		assert.EqualError(t, err, "failed to create")
	})
	t.Run("test create failure due to invalid file", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		instance := New(mockedController)
		err := CreateAnomalyDetector(instance, "testdata/create1.json", false)
		assert.EqualError(t, err, "failed to open file testdata/create1.json due to open testdata/create1.json: no such file or directory")
	})
	t.Run("test create failure due to empty file", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		instance := New(mockedController)
		err := CreateAnomalyDetector(instance, "", false)
		assert.EqualError(t, err, "file name cannot be empty")
	})
	t.Run("test create failure due to invalid file", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		instance := New(mockedController)
		err := CreateAnomalyDetector(instance, "testdata/invalid.txt", false)
		assert.EqualError(t, err, "file testdata/invalid.txt cannot be accepted due to invalid character 'i' looking for beginning of value")
	})
}
func TestHandler_DeleteAnomalyDetector(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	t.Run("test delete success", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().DeleteDetectorByName(ctx, "detector", false, true).Return(nil)
		instance := New(mockedController)
		err := DeleteAnomalyDetectorByNamePattern(instance, "detector", false)
		assert.NoError(t, err)
	})
	t.Run("test delete failure", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().DeleteDetectorByName(ctx, "detector", false, true).Return(errors.New("failed to delete"))
		instance := New(mockedController)
		err := DeleteAnomalyDetectorByNamePattern(instance, "detector", false)
		assert.EqualError(t, err, "failed to delete")
	})
}

func TestHandler_StartAnomalyDetector(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	t.Run("test start success", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().StartDetectorByName(ctx, "detector", true).Return(nil)
		instance := New(mockedController)
		err := StartAnomalyDetectorByNamePattern(instance, "detector")
		assert.NoError(t, err)
	})
	t.Run("test start failure", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().StartDetectorByName(ctx, "detector", true).Return(errors.New("failed to start"))
		instance := New(mockedController)
		err := instance.StartAnomalyDetectorByNamePattern("detector")
		assert.EqualError(t, err, "failed to start")
	})
}

func TestHandler_StopAnomalyDetector(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	t.Run("test stop success", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().StopDetectorByName(ctx, "detector", true).Return(nil)
		instance := New(mockedController)
		err := StopAnomalyDetectorByNamePattern(instance, "detector")
		assert.NoError(t, err)
	})
	t.Run("test stop failure", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().StopDetectorByName(ctx, "detector", true).Return(errors.New("failed to stop"))
		instance := New(mockedController)
		err := instance.StopAnomalyDetectorByNamePattern("detector")
		assert.EqualError(t, err, "failed to stop")
	})
}
func TestHandler_StartAnomalyDetectorByID(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	t.Run("test start success", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().StartDetector(ctx, "detector").Return(nil)
		instance := New(mockedController)
		err := StartAnomalyDetectorByID(instance, "detector")
		assert.NoError(t, err)
	})
	t.Run("test start failure", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().StartDetector(ctx, "detector").Return(errors.New("failed to start"))
		instance := New(mockedController)
		err := instance.StartAnomalyDetectorByID("detector")
		assert.EqualError(t, err, "failed to start")
	})
}

func TestHandler_StopAnomalyDetectorByID(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	t.Run("test stop success", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().StopDetector(ctx, "detector").Return(nil)
		instance := New(mockedController)
		err := StopAnomalyDetectorByID(instance, "detector")
		assert.NoError(t, err)
	})
	t.Run("test stop failure", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().StopDetector(ctx, "detector").Return(errors.New("failed to stop"))
		instance := New(mockedController)
		err := instance.StopAnomalyDetectorByID("detector")
		assert.EqualError(t, err, "failed to stop")
	})
}

func TestGenerateAnomalyDetector(t *testing.T) {
	t.Run("test generate success", func(t *testing.T) {
		expected := ad.CreateDetectorRequest{
			Name:        "Detector Name",
			Description: "A brief description",
			TimeField:   "",
			Index:       []string{},
			Features: []ad.FeatureRequest{
				{
					AggregationType: []string{"count"},
					Enabled:         false,
					Field:           []string{},
				},
			},
			Filter:         []byte("{}"),
			Interval:       "10m",
			Delay:          "1m",
			Start:          false,
			PartitionField: mapper.StringToStringPtr(""),
		}
		res, err := GenerateAnomalyDetector()
		assert.NoError(t, err)
		var actual ad.CreateDetectorRequest
		assert.NoError(t, json.Unmarshal(res, &actual))
		assert.EqualValues(t, expected, actual)
	})
}

func TestHandler_GetAnomalyDetectorByNamePattern(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	detectorOutput := []*ad.DetectorOutput{
		{
			ID:          "detectorID",
			Name:        "detector",
			Description: "Test detector",
			TimeField:   "timestamp",
			Index:       []string{"order*"},
			Features: []ad.Feature{
				{
					Name:             "total_order",
					Enabled:          true,
					AggregationQuery: []byte(`{"total_order":{"sum":{"field":"value"}}}`),
				},
			},
			Filter:        []byte(`{"bool" : {"filter" : [{"exists" : {"field" : "value","boost" : 1.0}}],"adjust_pure_negative" : true,"boost" : 1.0}}`),
			Interval:      "5m",
			Delay:         "1m",
			LastUpdatedAt: 1589441737319,
			SchemaVersion: 0,
		},
	}
	defer mockCtrl.Finish()
	t.Run("test get success", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().GetDetectorsByName(ctx, "detector", true).Return(detectorOutput, nil)
		instance := New(mockedController)
		result, err := GetAnomalyDetectorsByNamePattern(instance, "detector")
		assert.NoError(t, err)
		assert.EqualValues(t, detectorOutput, result)
	})
	t.Run("test get failure", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().GetDetectorsByName(ctx, "detector", true).Return(nil, errors.New("failed to stop"))
		instance := New(mockedController)
		_, err := instance.GetAnomalyDetectorsByNamePattern("detector")
		assert.EqualError(t, err, "failed to stop")
	})
}

func TestHandler_GetAnomalyDetectorByID(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	detectorOutput := &ad.DetectorOutput{
		ID:          "detectorID",
		Name:        "detector",
		Description: "Test detector",
		TimeField:   "timestamp",
		Index:       []string{"order*"},
		Features: []ad.Feature{
			{
				Name:             "total_order",
				Enabled:          true,
				AggregationQuery: []byte(`{"total_order":{"sum":{"field":"value"}}}`),
			},
		},
		Filter:        []byte(`{"bool" : {"filter" : [{"exists" : {"field" : "value","boost" : 1.0}}],"adjust_pure_negative" : true,"boost" : 1.0}}`),
		Interval:      "5m",
		Delay:         "1m",
		LastUpdatedAt: 1589441737319,
		SchemaVersion: 0,
	}
	defer mockCtrl.Finish()
	t.Run("test get by id success", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().GetDetector(ctx, "detectorID").Return(detectorOutput, nil)
		instance := New(mockedController)
		result, err := GetAnomalyDetectorByID(instance, "detectorID")
		assert.NoError(t, err)
		assert.EqualValues(t, detectorOutput, result)
	})
	t.Run("test get by id failure", func(t *testing.T) {
		mockedController := mocks.NewMockController(mockCtrl)
		mockedController.EXPECT().GetDetector(ctx, "detectorID").Return(nil, errors.New("failed to stop"))
		instance := New(mockedController)
		_, err := instance.GetAnomalyDetectorByID("detectorID")
		assert.EqualError(t, err, "failed to stop")
	})
}
