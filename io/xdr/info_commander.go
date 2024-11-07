package xdr

import (
	"fmt"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-management-lib/info"
	"github.com/go-logr/logr"
)

const (
	cmdDisableXDR       = "set-config:context=xdr;dc=%s;namespace=%s;action=remove"
	cmdEnableXDR        = "set-config:context=xdr;dc=%s;namespace=%s;action=add;rewind=%s"
	cmdBlockMRTWrites   = "%s%s"
	cmdUnBlockMRTWrites = "%s%s"
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

func (c *InfoCommander) DisableXDR(dc, namespace string) error {
	cmd := fmt.Sprintf(cmdDisableXDR, dc, namespace)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to disable xdr: %w", err)
	}

	return parseResultResponse(cmd, resp)
}

func (c *InfoCommander) EnableXDR(dc, namespace, rewind string) error {
	cmd := fmt.Sprintf(cmdEnableXDR, dc, namespace, rewind)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to enable xdr: %w", err)
	}

	return parseResultResponse(cmd, resp)
}

func (c *InfoCommander) BlockMRTWrites(dc, namespace string) error {
	cmd := fmt.Sprintf(cmdBlockMRTWrites, dc, namespace)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to block mrt writes: %w", err)
	}

	return parseResultResponse(cmd, resp)
}

func (c *InfoCommander) UnBlockMRTWrites(dc, namespace string) error {
	cmd := fmt.Sprintf(cmdUnBlockMRTWrites, dc, namespace)

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
