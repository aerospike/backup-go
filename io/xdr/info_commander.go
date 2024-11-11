package xdr

import (
	"fmt"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-management-lib/info"
	"github.com/go-logr/logr"
)

const (
	cmdDisableXDR = "set-config:context=xdr;dc=%s;namespace=%s;node-address-port=%s;connector=true;action=remove"
	//nolint:lll // Long info commands will be here.
	cmdEnableXDR        = "set-config:context=xdr;dc=%s;namespace=%s;node-address-port=%s;connector=true;action=add;rewind=%s"
	cmdBlockMRTWrites   = "health-stats"
	cmdUnBlockMRTWrites = "health-stats"
)

type InfoCommander struct {
	client *info.AsInfo
}

func NewInfoCommander(host string, port int, user, password string) *InfoCommander {
	asPolicy := aerospike.NewClientPolicy()
	asPolicy.User = user
	asPolicy.Password = password
	logger := logr.New(nil)
	asHost := aerospike.NewHost(host, port)
	client := info.NewAsInfo(logger, asHost, asPolicy)

	return &InfoCommander{client: client}
}

func (c *InfoCommander) DisableXDR(dc, hostPort, namespace string) error {
	cmd := fmt.Sprintf(cmdDisableXDR, dc, hostPort, namespace)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to disable xdr: %w", err)
	}

	return parseResultResponse(cmd, resp)
}

func (c *InfoCommander) EnableXDR(dc, hostPort, namespace, rewind string) error {
	cmd := fmt.Sprintf(cmdEnableXDR, dc, hostPort, namespace, rewind)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to enable xdr: %w", err)
	}

	return parseResultResponse(cmd, resp)
}

func (c *InfoCommander) BlockMRTWrites(_, _ string) error {
	cmd := cmdBlockMRTWrites

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to block mrt writes: %w", err)
	}

	return parseResultResponse(cmd, resp)
}

func (c *InfoCommander) UnBlockMRTWrites(_, _ string) error {
	cmd := cmdUnBlockMRTWrites

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to unblock mrt writes: %w", err)
	}

	return parseResultResponse(cmd, resp)
}

func parseResultResponse(cmd string, result map[string]string) error {
	v, ok := result[cmd]
	if !ok {
		return fmt.Errorf("no response for command %s", cmd)
	}

	if v != "ok" {
		return fmt.Errorf("command %s failed: %s", cmd, v)
	}

	return nil
}
