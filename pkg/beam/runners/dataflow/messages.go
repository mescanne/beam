package dataflow

import (
	"encoding/json"
	"google.golang.org/api/googleapi"
)

// newMsg creates a json-encoded RawMessage. Panics if encoding fails.
func newMsg(msg interface{}) googleapi.RawMessage {
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return googleapi.RawMessage(data)
}

// NOTE(herohde) 2/9/2017: most of the v1b3 messages are weakly-typed json
// blobs. We manually add them here for convenient and safer use.

// userAgent models Job/Environment/UserAgent. Example value:
//    "userAgent": {
//        "name": "Apache Beam SDK for Python",
//        "version": "0.6.0.dev"
//    },
type userAgent struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// version models Job/Environment/Version. Example value:
//    "version": {
//       "job_type": "PYTHON_BATCH",
//       "major": "5"
//    },
// See: dataflow_job_settings.gcl for range.
type version struct {
	JobType string `json:"job_type,omitempty"`
	Major   string `json:"major,omitempty"`
}

// properties models Step/Properties. Note that the valid subset of fields
// depend on the step kind.
type properties struct {
	UserName    string        `json:"user_name,omitempty"`
	DisplayData []displayData `json:"display_data,omitempty"`

	// Element []string  `json:"element,omitempty"`
	// UserFn string  `json:"user_fn,omitempty"`

	CustomSourceInputStep *customSourceInputStep      `json:"custom_source_step_input,omitempty"`
	ParallelInput         *outputReference            `json:"parallel_input,omitempty"`
	NonParallelInputs     map[string]*outputReference `json:"non_parallel_inputs,omitempty"`
	Format                string                      `json:"format,omitempty"`
	SerializedFn          string                      `json:"serialized_fn,omitempty"`
	OutputInfo            []output                    `json:"output_info,omitempty"`
}

type output struct {
	UserName   string    `json:"user_name,omitempty"`
	OutputName string    `json:"output_name,omitempty"`
	Encoding   *encoding `json:"encoding,omitempty"`
}

/*
    "@type": "kind:windowed_value",
    "component_encodings": [
      {
         "@type": "StrUtf8Coder$eNprYEpOLEhMzkiNT0pNzNVLzk9JLSqGUlzBJUWhJWkWziAeVyGDZmMhY20hU5IeAAajEkY=",
         "component_encodings": []
      },
      {
         "@type": "kind:global_window"
      }
   ],
   "is_wrapper": true
*/

// encoding defines the (structured) Coder.
type encoding struct {
	Type       string     `json:"@type,omitempty"`
	Components []encoding `json:"component_encodings,omitempty"`
	IsWrapper  bool       `json:"is_wrapper,omitempty"`
	IsPairLike bool       `json:"is_pair_like,omitempty"`
}

type customSourceInputStep struct {
	Spec customSourceInputStepSpec `json:"spec"`
	// TODO(herohde): metadata size estimate
}

type customSourceInputStepSpec struct {
	Type             string `json:"@type,omitempty"` // "CustomSourcesType"
	SerializedSource string `json:"serialized_source,omitempty"`
}

func newCustomSourceInputStep(serializedSource string) *customSourceInputStep {
	return &customSourceInputStep{
		Spec: customSourceInputStepSpec{
			Type:             "CustomSourcesType",
			SerializedSource: serializedSource,
		},
	}
}

type outputReference struct {
	Type       string `json:"@type,omitempty"` // "OutputReference"
	StepName   string `json:"step_name,omitempty"`
	OutputName string `json:"output_name,omitempty"`
}

func newOutputReference(step, output string) *outputReference {
	return &outputReference{
		Type:       "OutputReference",
		StepName:   step,
		OutputName: output,
	}
}

type displayData struct {
	Key        string `json:"key,omitempty"`
	Label      string `json:"label,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	ShortValue string `json:"shortValue,omitempty"`
	Type       string `json:"type,omitempty"`
	Value      string `json:"value,omitempty"`
}

/*
type displayDataInt struct {
	displayData
	Value int `json:"value,omitempty"`
}
*/
