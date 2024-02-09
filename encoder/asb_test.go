package encoder

import (
	"backuplib/models"
	"reflect"
	"testing"
)

func Test_escapeASBS(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "positive no escape",
			args: args{
				s: "hello",
			},
			want: "hello",
		},
		{
			name: "positive escape",
			args: args{
				s: "h el\\lo\n",
			},
			want: "h\\ el\\\\lo\\\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeASBS(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("escapeASB() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_encodeSIndexToASB(t *testing.T) {
	type args struct {
		sindex *models.SecondaryIndex
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "positive sindex no set or context",
			args: args{
				sindex: &models.SecondaryIndex{
					Namespace: "ns",
					Name:      "name",
					IndexType: models.BinSIndex,
					Path: models.SIndexPath{
						BinName: "bin",
						BinType: models.StringSIDataType,
					},
				},
			},
			want: []byte("* i ns  name N 1 bin S\n"),
		},
		{
			name: "positive escaped sindex no context",
			args: args{
				sindex: &models.SecondaryIndex{
					Namespace: "n s",
					Name:      "name\n",
					Set:       "se\\t",
					IndexType: models.BinSIndex,
					Path: models.SIndexPath{
						BinName: " bin",
						BinType: models.StringSIDataType,
					},
				},
			},
			want: []byte("* i n\\ s se\\\\t name\\\n N 1 \\ bin S\n"),
		},
		{
			name: "positive sindex with set and context",
			args: args{
				sindex: &models.SecondaryIndex{
					Namespace: "ns",
					Name:      "name",
					Set:       "set",
					IndexType: models.BinSIndex,
					Path: models.SIndexPath{
						BinName:    "bin",
						BinType:    models.StringSIDataType,
						B64Context: "context",
					},
				},
			},
			want: []byte("* i ns set name N 1 bin S context\n"),
		},
		{
			name: "negative sindex is nil",
			args: args{
				sindex: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeSIndexToASB(tt.args.sindex)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeSIndexToASB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("encodeSIndexToASB() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}
