package processors

import (
	"log/slog"
	"math"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	cltime "github.com/aerospike/backup-go/internal/citrusleaf_time"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
)

func TestProcessorTTL_Process(t *testing.T) {
	key, aerr := aerospike.NewKey("test", "test", "test")
	if aerr != nil {
		t.Fatal(aerr)
	}

	type fields struct {
		getNow  func() cltime.CLTime
		expired *atomic.Uint64
	}
	type args struct {
		token *models.Token
	}
	tests := []struct {
		fields      fields
		args        args
		want        *models.Token
		name        string
		wantErr     bool
		wantExpired uint64
	}{
		{
			name: "Test positive Process expired",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 100}
				},
				expired: &atomic.Uint64{},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &aerospike.Record{
							Key: key,
						},
						VoidTime: 100,
					},
				},
			},
			wantErr:     true,
			wantExpired: 1,
		},
		{
			name: "Test positive Process expired v2",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 200}
				},
				expired: &atomic.Uint64{},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &aerospike.Record{
							Key: key,
						},
						VoidTime: 100,
					},
				},
			},
			wantErr:     true,
			wantExpired: 1,
		},
		{
			name: "Test positive token is not a record",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 200}
				},
				expired: &atomic.Uint64{},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeSIndex,
				},
			},
			want: &models.Token{
				Type: models.TokenTypeSIndex,
			},
			wantErr:     false,
			wantExpired: 0,
		},
		{
			name: "Test positive Process",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
				expired: &atomic.Uint64{},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &aerospike.Record{
							Key: key,
						},
						VoidTime: 100,
					},
				},
			},
			want: &models.Token{
				Type: models.TokenTypeRecord,
				Record: models.Record{
					Record: &aerospike.Record{
						Expiration: 50,
						Key:        key,
					},
					VoidTime: 100,
				},
			},
			wantErr:     false,
			wantExpired: 0,
		},
		{
			name: "Test positive Process never expire",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
				expired: &atomic.Uint64{},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &aerospike.Record{
							Key: key,
						},
						VoidTime: models.VoidTimeNeverExpire,
					},
				},
			},
			want: &models.Token{
				Type: models.TokenTypeRecord,
				Record: models.Record{
					Record: &aerospike.Record{
						Expiration: models.ExpirationNever,
						Key:        key,
					},
					VoidTime: models.VoidTimeNeverExpire,
				},
			},
			wantErr:     false,
			wantExpired: 0,
		},
		{
			name: "Test negative time difference too large",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 1}
				},
				expired: &atomic.Uint64{},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &aerospike.Record{
							Key: key,
						},
						VoidTime: math.MaxInt64,
					},
				},
			},
			wantErr:     true,
			wantExpired: 0,
		},
		{
			name: "Test negative time difference too large",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 1}
				},
				expired: &atomic.Uint64{},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &aerospike.Record{
							Key: key,
						},
						VoidTime: math.MaxInt64,
					},
				},
			},
			wantErr:     true,
			wantExpired: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &expirationSetter{
				getNow:  tt.fields.getNow,
				expired: tt.fields.expired,
				logger:  slog.Default(),
			}
			got, err := p.Process(tt.args.token)
			if (err != nil) != tt.wantErr {
				t.Errorf("ProcessorTTL.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.wantExpired, p.expired.Load())
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ProcessorTTL.Process() = %v, want %v", got, tt.want)
			}
		})
	}
}
