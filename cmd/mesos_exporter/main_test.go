package main

import (
	"encoding/json"
	"github.com/thinker0/mesos_exporter/pkg/mesos"
	"reflect"
	"testing"
)

func TestPortRange_UnmarshalJSON(t *testing.T) {
	for i, tt := range []struct {
		data string
		want mesos.Ranges
		err  error
	}{
		{"[]", nil, nil},
		{`"[]"`, nil, nil},
		{`"[0-15]"`, mesos.Ranges{{0, 15}}, nil},
		{`"[0-15, 17-20]"`, mesos.Ranges{{0, 15}, {17, 20}}, nil},
	} {
		var rs mesos.Ranges
		if err := json.Unmarshal([]byte(tt.data), &rs); !reflect.DeepEqual(err, tt.err) {
			t.Errorf("test #%d: got err: %v, want: %v", i, err, tt.want)
		}

		if got := rs; !reflect.DeepEqual(got, tt.want) {
			t.Errorf("test #%d: got: %v, want: %v", i, got, tt.want)
		}
	}
}
