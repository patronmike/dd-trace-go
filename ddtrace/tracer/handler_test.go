package tracer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

// makeSpan returns a span, adding n entries to meta and metrics each.
func makeSpan(n int) *span {
	s := newSpan("encodeName", "encodeService", "encodeResource", random.Uint64(), random.Uint64(), random.Uint64())
	for i := 0; i < n; i++ {
		istr := fmt.Sprintf("%0.10d", i)
		s.Meta[istr] = istr
		s.Metrics[istr] = float64(i)
	}
	return s
}

func TestEncodeFloat(t *testing.T) {
	for _, tt := range []struct {
		f      float64
		expect string
	}{
		{
			9.9999999999999990e20,
			"999999999999999900000",
		},
		{
			9.9999999999999999e20,
			"1e+21",
		},
		{
			-9.9999999999999990e20,
			"-999999999999999900000",
		},
		{
			-9.9999999999999999e20,
			"-1e+21",
		},
		{
			0.000001,
			"0.000001",
		},
		{
			0.0000009,
			"9e-7",
		},
		{
			-0.000001,
			"-0.000001",
		},
		{
			-0.0000009,
			"-9e-7",
		},
		{
			math.NaN(),
			"NaN",
		},
		{
			math.Inf(-1),
			"-Infinity",
		},
		{
			math.Inf(1),
			"Infinity",
		},
	} {
		t.Run(tt.expect, func(t *testing.T) {
			assert.Equal(t, tt.expect, string(encodeFloat(nil, tt.f)))
		})
	}

}

func TestLogWriter(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		assert := assert.New(t)
		var buf bytes.Buffer
		h := newLogTraceWriter(newConfig())
		h.w = &buf
		s := makeSpan(0)
		for i := 0; i < 20; i++ {
			h.add([]*span{s, s})
		}
		h.flush()
		v := struct{ Traces [][]map[string]interface{} }{}
		d := json.NewDecoder(&buf)
		err := d.Decode(&v)
		assert.NoError(err)
		assert.Len(v.Traces, 20, "Expected 20 traces, but have %d", len(v.Traces))
		for _, t := range v.Traces {
			assert.Len(t, 2, "Expected 2 spans, but have %d", len(t))
		}
		err = d.Decode(&v)
		assert.Equal(io.EOF, err)
	})
	t.Run("inf+nan", func(t *testing.T) {
		assert := assert.New(t)
		var buf bytes.Buffer
		h := newLogTraceWriter(newConfig())
		h.w = &buf
		s := makeSpan(0)
		s.Metrics["nan"] = math.NaN()
		s.Metrics["+inf"] = math.Inf(1)
		s.Metrics["-inf"] = math.Inf(-1)
		h.add([]*span{s})
		h.flush()
		json := string(buf.Bytes())
		assert.Contains(json, `"nan":NaN`)
		assert.Contains(json, `"+inf":Infinity`)
		assert.Contains(json, `"-inf":-Infinity`)
	})
}

func TestLogWriterOverflow(t *testing.T) {
	log.UseLogger(new(testLogger))
	t.Run("single-too-big", func(t *testing.T) {
		assert := assert.New(t)
		var buf bytes.Buffer
		var tg testStatsdClient
		h := newLogTraceWriter(newConfig(withStatsdClient(&tg)))
		h.w = &buf
		s := makeSpan(10000)
		h.add([]*span{s})
		h.flush()
		v := struct{ Traces [][]map[string]interface{} }{}
		d := json.NewDecoder(&buf)
		err := d.Decode(&v)
		assert.Equal(io.EOF, err)
		assert.Contains(tg.CallNames(), "datadog.tracer.traces_dropped")
	})

	t.Run("split", func(t *testing.T) {
		assert := assert.New(t)
		var buf bytes.Buffer
		var tg testStatsdClient
		h := newLogTraceWriter(newConfig(withStatsdClient(&tg)))
		h.w = &buf
		s := makeSpan(10)
		var trace []*span
		for i := 0; i < 500; i++ {
			trace = append(trace, s)
		}
		h.add(trace)
		h.flush()
		v := struct{ Traces [][]map[string]interface{} }{}
		d := json.NewDecoder(&buf)
		err := d.Decode(&v)
		assert.NoError(err)
		assert.Len(v.Traces, 1, "Expected 1 trace, but have %d", len(v.Traces))
		spann := len(v.Traces[0])
		err = d.Decode(&v)
		assert.NoError(err)
		assert.Len(v.Traces, 1, "Expected 1 trace, but have %d", len(v.Traces))
		spann += len(v.Traces[0])
		assert.Equal(500, spann)
		err = d.Decode(&v)
		assert.Equal(io.EOF, err)
	})

	t.Run("two-large", func(t *testing.T) {
		assert := assert.New(t)
		var buf bytes.Buffer
		h := newLogTraceWriter(newConfig())
		h.w = &buf
		s := makeSpan(4000)
		h.add([]*span{s})
		h.add([]*span{s})
		h.flush()
		v := struct{ Traces [][]map[string]interface{} }{}
		d := json.NewDecoder(&buf)
		err := d.Decode(&v)
		assert.NoError(err)
		assert.Len(v.Traces, 1, "Expected 1 trace, but have %d", len(v.Traces))
		assert.Len(v.Traces[0], 1, "Expected 1 span, but have %d", len(v.Traces[0]))
		err = d.Decode(&v)
		assert.NoError(err)
		assert.Len(v.Traces, 1, "Expected 1 trace, but have %d", len(v.Traces))
		assert.Len(v.Traces[0], 1, "Expected 1 span, but have %d", len(v.Traces[0]))
		err = d.Decode(&v)
		assert.Equal(io.EOF, err)
	})
}

func BenchmarkJsonEncodeSpan(b *testing.B) {
	s := makeSpan(10)
	s.Metrics["nan"] = math.NaN()
	s.Metrics["+inf"] = math.Inf(1)
	s.Metrics["-inf"] = math.Inf(-1)
	h := &logTraceWriter{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.resetPayload()
		h.encodeSpan(s)
	}
}

func BenchmarkJsonEncodeFloat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ba = make([]byte, 25)
		bs := ba[:0]
		encodeFloat(bs, float64(1e-9))
	}
}
