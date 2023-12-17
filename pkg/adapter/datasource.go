// Copyright 2023 SGNL.ai, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	framework "github.com/sgnl-ai/adapter-framework"
	api_adapter_v1 "github.com/sgnl-ai/adapter-framework/api/adapter/v1"
)

const (
	// SCAFFOLDING:
	// Update the set of valid entity types supported by this adapter.
	Teams string = "teams"
)

// Entity contains entity specific information, such as the entity's unique ID attribute and the
// endpoint to query that entity.
type Entity struct {
	// SCAFFOLDING:
	// Add or remove fields as needed. This should be used to store entity specific information
	// such as the entity's unique ID attribute name and the endpoint to query that entity.

	// uniqueIDAttrExternalID is the external ID of the entity's uniqueId attribute.
	uniqueIDAttrExternalID string
}

// Datasource directly implements a Client interface to allow querying
// an external datasource.
type Datasource struct {
	Client *http.Client
}

type DatasourceResponse struct {
	// SCAFFOLDING:
	// Add or remove fields as needed. This should be used to unmarshal the response from the datasource.

	// SCAFFOLDING:
	// Replace `objects` with the field name in the datasource response that contains the
	// list of objects. Update the datatype is needed.
	Objects []map[string]any `json:"teams"`
	More bool `json:"more"`
	Limit int64 `json:"limit"`
	Offset int64 `json:"offset"`
}

type Team struct {
	ID string `json:"id"`
}

var (
	// SCAFFOLDING:
	// Using the consts defined above, update the set of valid entity types supported by this adapter.

	// ValidEntityExternalIDs is a map of valid external IDs of entities that can be queried.
	// The map value is the Entity struct which contains the unique ID attribute.
	ValidEntityExternalIDs = map[string]Entity{
		Teams: {
			uniqueIDAttrExternalID: "id",
		},
	}
)

// NewClient returns a Client to query the datasource.
func NewClient(timeout int) Client {
	return &Datasource{
		Client: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
	}
}

func (d *Datasource) GetPage(ctx context.Context, request *Request) (*Response, *framework.Error) {
	var req *http.Request
	// SCAFFOLDING:
	// Populate the request with the appropriate path, headers, and query parameters to query the
	// datasource.
	offset, offsetErr := parseCursor(request.Cursor)
	if offsetErr != nil {
		return nil, offsetErr
	}
	url := fmt.Sprintf("%s/%s?offset=%d&limit=%d", request.BaseURL, request.EntityExternalID, offset, request.PageSize)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &framework.Error{
			Message: "Failed to create HTTP request to datasource.",
			Code:    api_adapter_v1.ErrorCode_ERROR_CODE_INTERNAL,
		}
	}

	// Timeout API calls that take longer than 5 seconds
	apiCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req = req.WithContext(apiCtx)

	// SCAFFOLDING:
	// Add headers to the request, if any.
	req.Header.Add("Accept", "application/vnd.pagerduty+json;version=2")
	req.Header.Add("Authorization", request.HTTPAuthorization)
	req.Header.Add("Content-Type", "application/json")

	res, err := d.Client.Do(req)
	if err != nil {
		return nil, &framework.Error{
			Message: "Failed to send request to datasource.",
			Code:    api_adapter_v1.ErrorCode_ERROR_CODE_INTERNAL,
		}
	}

	response := &Response{
		StatusCode:       res.StatusCode,
		RetryAfterHeader: res.Header.Get("Retry-After"),
	}

	if res.StatusCode != http.StatusOK {
		return response, nil
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, &framework.Error{
			Message: "Failed to read response body.",
			Code:    api_adapter_v1.ErrorCode_ERROR_CODE_DATASOURCE_FAILED,
		}
	}

	objects, nextCursor, parseErr := ParseResponse(body)
	if parseErr != nil {
		return nil, parseErr
	}

	response.Objects = objects
	response.NextCursor = nextCursor

	return response, nil
}

func parseCursor(cursor string) (int64, *framework.Error) {
	if cursor == "" {
		// Return a default value, or handle the case as needed
		return 0, nil
	}

	parsedOffset, parseErr := strconv.ParseInt(cursor, 10, 64)
	if parseErr != nil {
		// Handle the error if parsing fails
		return 0, &framework.Error{
			Message: "Request cursor conversion to int64 failed.",
			Code:    api_adapter_v1.ErrorCode_ERROR_CODE_INVALID_PAGE_REQUEST_CONFIG,
		}
	}

	return parsedOffset, nil
}

func ParseResponse(body []byte) (objects []map[string]any, nextCursor string, err *framework.Error) {
	var data *DatasourceResponse

	unmarshalErr := json.Unmarshal(body, &data)
	if unmarshalErr != nil {
		return nil, "", &framework.Error{
			Message: fmt.Sprintf("Failed to unmarshal the datasource response: %v.", unmarshalErr),
			Code:    api_adapter_v1.ErrorCode_ERROR_CODE_INTERNAL,
		}
	}

	// SCAFFOLDING:
	// Add necessary validations to check if the response from the datasource is what is expected.


	// SCAFFOLDING:
	// Populate nextCursor with the cursor returned from the datasource, if present.

	nextCursor = ""
	if data.More {
		nextCursor = strconv.FormatInt(data.Offset+data.Limit, 10)
	}
	return data.Objects, nextCursor, nil
}
