package aerospike

import (
	"fmt"
	"strings"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-management-lib/info"
	"github.com/go-logr/logr"
)

const (
	cmdCreateXDRDC        = "set-config:context=xdr;dc=%s;action=create"
	cmdCreateXDRNode      = "set-config:context=xdr;dc=%s;node-address-port=%s;connector=true;action=add"
	cmdCreateXDRNamespace = "set-config:context=xdr;dc=%s;namespace=%s;action=add;rewind=%s"

	cmdRemoveXDRNamespace = "set-config:context=xdr;dc=%s;namespace=%s;action=remove"
	cmdRemoveXDRNode      = "set-config:context=xdr;dc=%s;node-address-port=%s;action=remove"
	cmdRemoveXDRDC        = "set-config:context=xdr;dc=%s;action=remove"

	cmdStatistics = "statistics"

	cmdBlockMRTWrites   = "namespaces"
	cmdUnBlockMRTWrites = "namespaces"

	cmdRespErrPrefix = "ERROR"
)

type InfoCommander struct {
	client *info.AsInfo
}

func NewInfoCommander(asHost *aerospike.Host, asPolicy *aerospike.ClientPolicy) *InfoCommander {
	logger := logr.New(nil)
	client := info.NewAsInfo(logger, asHost, asPolicy)

	return &InfoCommander{client: client}
}

// StartXDR creates xdr config and starts replication.
func (c *InfoCommander) StartXDR(dc, hostPort, namespace, rewind string) error {
	if err := c.createXDRDC(dc); err != nil {
		return err
	}

	if err := c.createXDRNode(dc, hostPort); err != nil {
		return err
	}

	if err := c.createXDRNamespace(dc, namespace, rewind); err != nil {
		return err
	}

	return nil
}

// StopXDR disable replication and remove xdr config.
func (c *InfoCommander) StopXDR(dc, hostPort, namespace string) error {
	if err := c.removeXDRNamespace(dc, namespace); err != nil {
		return err
	}

	if err := c.removeXDRNode(dc, hostPort); err != nil {
		return err
	}

	if err := c.removeXDRDC(dc); err != nil {
		return err
	}

	return nil
}

func (c *InfoCommander) createXDRDC(dc string) error {
	cmd := fmt.Sprintf(cmdCreateXDRDC, dc)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr dc: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr dc response: %w", err)
	}

	return nil
}

func (c *InfoCommander) createXDRNode(dc, hostPort string) error {
	cmd := fmt.Sprintf(cmdCreateXDRNode, dc, hostPort)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr node: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr node response: %w", err)
	}

	return nil
}

func (c *InfoCommander) createXDRNamespace(dc, namespace, rewind string) error {
	cmd := fmt.Sprintf(cmdCreateXDRNamespace, dc, namespace, rewind)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to create xdr namesapce: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse create xdr namesapce response: %w", err)
	}

	return nil
}

func (c *InfoCommander) removeXDRNamespace(dc, namespace string) error {
	cmd := fmt.Sprintf(cmdRemoveXDRNamespace, dc, namespace)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to remove xdr namespace: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse remove xdr namespace response: %w", err)
	}

	return nil
}

func (c *InfoCommander) removeXDRNode(dc, hostPort string) error {
	cmd := fmt.Sprintf(cmdRemoveXDRNode, dc, hostPort)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to remove xdr node: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse remove xdr node response: %w", err)
	}

	return nil
}

func (c *InfoCommander) removeXDRDC(dc string) error {
	cmd := fmt.Sprintf(cmdRemoveXDRDC, dc)

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to remove xdr dc: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse remove xdr dc response: %w", err)
	}

	return nil
}

// BlockMRTWrites blocks MRT writes on cluster.
func (c *InfoCommander) BlockMRTWrites(_, _ string) error {
	cmd := cmdBlockMRTWrites

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to block mrt writes: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse block mrt writes response: %w", err)
	}

	return nil
}

// UnBlockMRTWrites unblocks MRT writes on cluster.
func (c *InfoCommander) UnBlockMRTWrites(_, _ string) error {
	cmd := cmdUnBlockMRTWrites

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to unblock mrt writes: %w", err)
	}

	if _, err = parseResultResponse(cmd, resp); err != nil {
		return fmt.Errorf("failed to parse unblock mrt writes response: %w", err)
	}

	return nil
}

func (c *InfoCommander) Statistics() error {
	cmd := cmdStatistics

	resp, err := c.client.RequestInfo(cmd)
	if err != nil {
		return fmt.Errorf("failed to get statistics: %w", err)
	}

	fmt.Println("============STATISTICS================")
	fmt.Println("client_connections=", resp["client_connections"])
	fmt.Println("client_connections_opened=", resp["client_connections_opened"])
	fmt.Println("client_connections_closed=", resp["client_connections_closed"])

	return nil
}

func parseResultResponse(cmd string, result map[string]string) (string, error) {
	v, ok := result[cmd]
	if !ok {
		return "", fmt.Errorf("no response for command %s", cmd)
	}

	if strings.Contains(v, cmdRespErrPrefix) {
		return "", fmt.Errorf("command %s failed: %s", cmd, v)
	}

	return v, nil
}
