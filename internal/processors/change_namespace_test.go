package processors

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/assert"
)

func TestChangeNamespaceProcessor(t *testing.T) {
	restoreNamespace := models.RestoreNamespace{
		Source:      ptr.String("sourceNS"),
		Destination: ptr.String("destinationNS"),
	}

	key, _ := aerospike.NewKey(*restoreNamespace.Source, "set", 1)
	invalidKey, _ := aerospike.NewKey("otherNs", "set", 1)

	tests := []struct {
		restoreNS    *models.RestoreNamespace
		initialToken *models.Token
		name         string
		wantErr      bool
	}{
		{
			name:      "nil restore Namespace",
			restoreNS: nil,
			initialToken: models.NewRecordToken(models.Record{
				Record: &aerospike.Record{
					Key: key,
				},
			}, 0),
			wantErr: false,
		},
		{
			name:         "non-record Token Type",
			restoreNS:    &restoreNamespace,
			initialToken: models.NewUDFToken(nil, 0),
			wantErr:      false,
		},
		{
			name:      "invalid source namespace",
			restoreNS: &restoreNamespace,
			initialToken: models.NewRecordToken(models.Record{
				Record: &aerospike.Record{
					Key: invalidKey,
				},
			}, 0),
			wantErr: true,
		},
		{
			name:      "valid process",
			restoreNS: &restoreNamespace,
			initialToken: models.NewRecordToken(models.Record{
				Record: &aerospike.Record{
					Key: key,
				},
			}, 0),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewChangeNamespace(tt.restoreNS)
			gotToken, err := p.Process(tt.initialToken)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.initialToken.Type == models.TokenTypeRecord && tt.restoreNS != nil {
					assert.Equal(t, *tt.restoreNS.Destination, gotToken.Record.Key.Namespace())
				}
			}
		})
	}
}
