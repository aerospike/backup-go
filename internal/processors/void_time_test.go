package processors

import (
	"reflect"
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	cltime "github.com/aerospike/backup-go/encoding/citrusleaf_time"
	"github.com/aerospike/backup-go/models"
)

func Test_processorVoidTime_Process(t *testing.T) {
	type fields struct {
		getNow func() cltime.CLTime
	}
	type args struct {
		token *models.Token
	}
	tests := []struct {
		fields  fields
		args    args
		want    *models.Token
		name    string
		wantErr bool
	}{
		{
			name: "Test positive Process",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &aerospike.Record{
							Expiration: 100,
						},
					},
				},
			},
			want: &models.Token{
				Type: models.TokenTypeRecord,
				Record: models.Record{
					Record: &aerospike.Record{
						Expiration: 100,
					},
					VoidTime: 150,
				},
			},
			wantErr: false,
		},
		{
			name: "Test positive never expire",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &aerospike.Record{
							Expiration: models.ExpirationNever,
						},
					},
				},
			},
			want: &models.Token{
				Type: models.TokenTypeRecord,
				Record: models.Record{
					Record: &aerospike.Record{
						Expiration: models.ExpirationNever,
					},
					VoidTime: models.VoidTimeNeverExpire,
				},
			},
			wantErr: false,
		},
		{
			name: "Test positive token is not a record",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeSIndex,
				},
			},
			want: &models.Token{
				Type: models.TokenTypeSIndex,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &voidTimeSetter{
				getNow: tt.fields.getNow,
			}
			got, err := p.Process(tt.args.token)
			if (err != nil) != tt.wantErr {
				t.Errorf("voidTimeSetter.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("voidTimeSetter.Process() = %v, want %v", got, tt.want)
			}
		})
	}
}
