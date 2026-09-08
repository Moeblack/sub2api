package service

import (
	"context"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSNativeTracking_RejectedCreateDoesNotReplaceLastResponseOwner(t *testing.T) {
	client, upstream := newNativeRuntimeTestConn(), newNativeRuntimeTestConn()
	parent, err := nativeRuntimeTestPrepare(1, []byte(`{"type":"response.create","stream_id":"main","model":"gpt-6-astra","input":"first"}`))
	require.NoError(t, err)
	parent.streamID, parent.startedAt = "main", time.Now()
	lane := &openAIWSNativeLane{active: parent}
	runtime := &openAIWSNativeRuntime{
		ctx: context.Background(), client: client, upstream: upstream,
		options: openAIWSNativeOptions{prepare: nativeRuntimeTestPrepare, writeTimeout: time.Second},
		lanes:   map[string]*openAIWSNativeLane{"main": lane}, laneOrder: []string{"main"},
		responses: make(map[string]*openAIWSNativeTurn), nextTurn: 1, active: 1,
	}
	upstreamEvent := func(payload string) {
		t.Helper()
		require.NoError(t, runtime.handleUpstream(openAIWSNativeFrame{messageType: coderws.MessageText, payload: []byte(payload)}))
		nativeRuntimeTestRead(t, client)
	}
	create := func(payload string) {
		t.Helper()
		require.NoError(t, runtime.handleClient(openAIWSNativeFrame{messageType: coderws.MessageText, payload: []byte(payload)}))
		require.NoError(t, runtime.dispatch())
		nativeRuntimeTestRead(t, upstream)
	}

	upstreamEvent(nativeRuntimeTestCreated("main", "resp_parent", "gpt-6-astra"))
	upstreamEvent(nativeRuntimeTestCompleted("main", "resp_parent", "gpt-6-astra", 5, 2, 0))
	require.Same(t, parent, lane.latest)
	require.Same(t, parent, runtime.responses["resp_parent"])

	create(`{"type":"response.create","stream_id":"main","model":"gpt-6-astra","previous_response_id":"resp_parent","input":"rejected request"}`)
	upstreamEvent(`{"type":"error","stream_id":"main","status":400,"error":{"type":"invalid_request_error","code":"invalid_request_error","message":"create rejected before response allocation"}}`)
	require.Nil(t, lane.active)
	require.Zero(t, runtime.active)
	require.Same(t, parent, lane.latest, "a rejected create never acquired a response ID and cannot replace the lane's actual parent")
	require.Same(t, parent, runtime.responses["resp_parent"])
	require.Len(t, runtime.responses, 1)

	create(`{"type":"response.create","stream_id":"main","model":"gpt-6-astra","previous_response_id":"resp_parent","input":"valid followup"}`)
	upstreamEvent(nativeRuntimeTestCreated("main", "resp_next", "gpt-6-astra"))
	require.Equal(t, "resp_next", lane.latest.responseID)
	require.Len(t, runtime.responses, 1, "the next allocated response must evict the actual previous parent, not an empty rejected-response ID")
	require.NotContains(t, runtime.responses, "resp_parent")
	require.Same(t, lane.active, runtime.responses["resp_next"])

	require.NoError(t, runtime.handleClient(openAIWSNativeFrame{
		messageType: coderws.MessageText,
		payload:     []byte(`{"type":"response.steer","previous_response_id":"resp_parent","input":"stale parent"}`),
	}))
	failure := nativeRuntimeTestRead(t, client)
	require.Equal(t, "response.steer.failed", failure.Get("type").String())
	require.Equal(t, "response_not_found", failure.Get("error.code").String())
	require.Equal(t, 2, upstream.writeCount(), "an evicted response ID must not retain control authority")
}
