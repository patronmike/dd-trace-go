// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package tracer

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

type traceWriter interface {
	// add adds a trace to the current payload being constructed by the handler.
	add([]*span)

	// flush causes the handler to send its current payload. The flush can happen asynchronously, but
	// the handler must be ready to accept new traces with add when flush returns.
	flush()

	// stop shuts down the traceWriter, ensuring all payloads are flushed.
	stop()
}

type agentTraceWriter struct {
	config *config

	payload *payload

	// climit limits the number of concurrent outgoing connections
	climit chan struct{}

	// wg waits for all goroutines to exit when stopping.
	wg sync.WaitGroup

	// prioritySampling refers to the tracer's priority sampler.
	prioritySampling *prioritySampler
}

var _ traceWriter = &agentTraceWriter{}

func newAgentTraceWriter(c *config, s *prioritySampler) *agentTraceWriter {
	return &agentTraceWriter{
		config:           c,
		payload:          newPayload(),
		climit:           make(chan struct{}, concurrentConnectionLimit),
		prioritySampling: s,
	}
}

func (h *agentTraceWriter) add(trace []*span) {
	if err := h.payload.push(trace); err != nil {
		h.config.statsd.Incr("datadog.tracer.traces_dropped", []string{"reason:encoding_error"}, 1)
		log.Error("error encoding msgpack: %v", err)
	}
	if h.payload.size() > payloadSizeLimit {
		h.config.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:size"}, 1)
		h.flush()
	}
}

func (h *agentTraceWriter) stop() {
	h.wg.Wait()
}

// flush will push any currently buffered traces to the server.
func (h *agentTraceWriter) flush() {
	if h.payload.itemCount() == 0 {
		return
	}
	h.wg.Add(1)
	h.climit <- struct{}{}
	go func(p *payload) {
		defer func(start time.Time) {
			<-h.climit
			h.wg.Done()
			h.config.statsd.Timing("datadog.tracer.flush_duration", time.Since(start), nil, 1)
		}(time.Now())
		size, count := p.size(), p.itemCount()
		log.Debug("Sending payload: size: %d traces: %d\n", size, count)
		rc, err := h.config.transport.send(p)
		if err != nil {
			h.config.statsd.Count("datadog.tracer.traces_dropped", int64(count), []string{"reason:send_failed"}, 1)
			log.Error("lost %d traces: %v", count, err)
		} else {
			h.config.statsd.Count("datadog.tracer.flush_bytes", int64(size), nil, 1)
			h.config.statsd.Count("datadog.tracer.flush_traces", int64(count), nil, 1)
			if err := h.prioritySampling.readRatesJSON(rc); err != nil {
				h.config.statsd.Incr("datadog.tracer.decode_error", nil, 1)
			}
		}
	}(h.payload)
	h.payload = newPayload()
}

type logTraceWriter struct {
	config    *config
	buf       bytes.Buffer
	hasTraces bool
	w         io.Writer
}

var _ traceWriter = &logTraceWriter{}

func newLogTraceWriter(c *config) *logTraceWriter {
	w := &logTraceWriter{
		config: c,
		w:      os.Stdout,
	}
	w.resetPayload()
	return w
}

const (
	// maxFloatLength is the maximum length that a string encoded by encodeFloat can be.
	// At 1e21 and beyond, floats are encoded to strings in exponential format. The longest
	// a string encoded by encodeFloat can be is 22 characters.
	maxFloatLength = 22

	// suffix is the final string that the trace writer has to append to a payload to close
	// the JSON.
	logPayloadSuffix = "]}\n"

	// logPayloadLimit is the maximum size log line allowed by cloudwatch
	logPayloadLimit = 256 * 1024
)

func (h *logTraceWriter) resetPayload() {
	h.buf.Reset()
	h.buf.WriteString(`{"traces": [`)
	h.hasTraces = false
}

// encodeFloat correctly encodes float64 to the format enforced by ES6
func encodeFloat(p []byte, f float64) []byte {
	if math.IsInf(f, -1) {
		return append(p, "-Infinity"...)
	} else if math.IsInf(f, 1) {
		return append(p, "Infinity"...)
	}
	abs := math.Abs(f)
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		p = strconv.AppendFloat(p, f, 'e', -1, 64)
		// clean up e-09 to e-9
		n := len(p)
		if n >= 4 && p[n-4] == 'e' && p[n-3] == '-' && p[n-2] == '0' {
			p[n-2] = p[n-1]
			p = p[:n-1]
		}
	} else {
		p = strconv.AppendFloat(p, f, 'f', -1, 64)
	}
	return p
}

func (h *logTraceWriter) encodeSpan(s *span) {
	var scratch [maxFloatLength]byte
	h.buf.WriteString(`{"trace_id":"`)
	h.buf.Write(strconv.AppendUint(scratch[:0], uint64(s.TraceID), 16))
	h.buf.WriteString(`","span_id":"`)
	h.buf.Write(strconv.AppendUint(scratch[:0], uint64(s.SpanID), 16))
	h.buf.WriteString(`","parent_id":"`)
	h.buf.Write(strconv.AppendUint(scratch[:0], uint64(s.ParentID), 16))
	h.buf.WriteString(`","name":"`)
	h.buf.WriteString(s.Name)
	h.buf.WriteString(`","resource":"`)
	h.buf.WriteString(s.Resource)
	h.buf.WriteString(`","error":`)
	h.buf.Write(strconv.AppendInt(scratch[:0], int64(s.Error), 10))
	h.buf.WriteString(`,"meta":{`)
	first := true
	for k, v := range s.Meta {
		if first {
			h.buf.WriteByte('"')
			first = false
		} else {
			h.buf.WriteString(`,"`)
		}
		h.buf.WriteString(k)
		h.buf.WriteString(`":"`)
		h.buf.WriteString(v)
		h.buf.WriteString(`"`)
	}
	h.buf.WriteString(`},"metrics":{`)
	first = true
	for k, v := range s.Metrics {
		if first {
			h.buf.WriteByte('"')
			first = false
		} else {
			h.buf.WriteString(`,"`)
		}
		h.buf.WriteString(k)
		h.buf.WriteString(`":`)
		h.buf.Write(encodeFloat(scratch[:0], v))
	}
	h.buf.WriteString(`},"start":`)
	h.buf.Write(strconv.AppendInt(scratch[:0], s.Start, 10))
	h.buf.WriteString(`,"duration":`)
	h.buf.Write(strconv.AppendInt(scratch[:0], s.Duration, 10))
	h.buf.WriteString(`,"service":"`)
	h.buf.WriteString(s.Service)
	h.buf.WriteString(`"}`)
}

type encodingError struct {
	cause      error
	dropReason string
}

func (h *logTraceWriter) writeTrace(trace []*span) (n int, err *encodingError) {
	startn := h.buf.Len()
	if !h.hasTraces {
		h.buf.WriteByte('[')
	} else {
		h.buf.WriteString(", [")
	}
	written := 0
	for i, s := range trace {
		n := h.buf.Len()
		if i > 0 {
			h.buf.WriteByte(',')
		}
		h.encodeSpan(s)
		if h.buf.Len() > logPayloadLimit-len(logPayloadSuffix) {
			if i == 0 {
				h.buf.Truncate(startn)
				if !h.hasTraces {
					// This is the first span of the first trace, and it's too big.
					return 0, &encodingError{cause: errors.New("span too large for payload"), dropReason: "trace_too_large"}
				}
				return 0, nil
			}
			h.buf.Truncate(n)
			break
		}
		written++
	}
	h.buf.WriteByte(']')
	h.hasTraces = true
	return written, nil
}

func (h *logTraceWriter) add(trace []*span) {
	for len(trace) > 0 {
		n, err := h.writeTrace(trace)
		if err != nil {
			log.Error("lost a trace: %s", err.cause)
			h.config.statsd.Count("datadog.tracer.traces_dropped", 1, []string{"reason:" + err.dropReason}, 1)
			return
		}
		trace = trace[n:]
		if len(trace) > 0 {
			h.flush()
		}
	}
}

func (h *logTraceWriter) stop() {}

// flush will write any buffered traces to standard output.
func (h *logTraceWriter) flush() {
	if !h.hasTraces {
		return
	}
	h.buf.WriteString(logPayloadSuffix)
	h.w.Write(h.buf.Bytes())
	h.resetPayload()
}
